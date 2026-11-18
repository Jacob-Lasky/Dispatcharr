"""Owned M3U reservations for Dispatcharr 0.34.1, including its live reload adapter.

The 0.33-0.34.1 reconnect path can reserve twice before publishing one channel token.
Keep assignment changes under a cross-worker lock, and commit each counter change
with a durable owner record. Maintenance only reclaims owners with no proxy,
owner lock or clients; it never stops a channel or guesses from throughput.
"""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
import ast
import hashlib
import inspect
import logging
import os
import socket
import sys
import time
import uuid
from pathlib import Path

VERSION = '0.2.0'  # Keep equal to tools/pool_guard/plugin.json and plugin.py.
GRACE_SECONDS = 180
INDEX = 'm3u:owned_slots'
WORKERS = 'm3u:pool_guard:workers'
_REGISTRY = '_dispatcharr_m3u_pool_guard'
# The installed Guard survives plugin module eviction on a force reload.
_scope = getattr(sys.modules.get(_REGISTRY), 'scope_var', None) or ContextVar('m3u_reservation_owner', default=None)
_held_locks = getattr(sys.modules.get(_REGISTRY), 'lock_var', None) or ContextVar('m3u_assignment_locks', default=frozenset())
logger = logging.getLogger('apps.m3u.reservation_guard')

SUPPORTED_VERSION = '0.34.1'
UPSTREAM_REVISION = '83f82bf388b72ec1e6cc356f1015a9812228b424'
# Pin every core file whose functions or loader contract the adapter relies on.
# Image builds set version.py independently of the source revision.
CORE_SHA256 = {
    'apps/m3u/connection_pool.py': 'e79c8d663818068d71b23b2404e9a313ce566bd6a51d4ae217211ac72fc83bfb',
    'apps/channels/models.py': 'dcfba15cf91c49260b3dae1227bd16166c6907e4e3be87d828da099473f30408',
    'apps/proxy/live_proxy/client_manager.py': '4d0c6c7a4a60fe7d7f97794b4c28b3b9d9703f2772964315a802a3fc4ac2cc9d',
    'apps/proxy/live_proxy/slot_release.py': '32134f939f7e53cd562140f9057df571e3b1d3852f4d806af64f7ad16723f7ac',
    'apps/proxy/live_proxy/views.py': '7ce96b220e246d05f74690b58a7bd765c2c41d316487aae471a5e46ba0cc1cf8',
    'apps/proxy/live_proxy/server.py': 'd9e43ec4899432a2eed2b31bd17cee02c36bcfc33dfd821194bdb8542b794ebf',
    'apps/proxy/live_proxy/output/ts/generator.py': 'ebb47c35ce4fa6ffc3baf1d9545089077da9ecafc9cc25f3dc8c326a0aa08904',
    'apps/proxy/live_proxy/output/fmp4/generator.py': 'a337030b2b560c54eac013ee2c26aaa4a974f696b55d2ef3d7d2023298e2af5e',
    'apps/plugins/loader.py': '84d6443927e97a1973ce83cd99766dc7d0e6f768e9ad0d9dceba1ba3011ac719',
}


def compatibility_errors(root=Path('/app')):
    """Fail closed on a changed upstream version or hooked implementation."""
    root = Path(root)
    errors = []
    try:
        nodes = ast.parse((root / 'version.py').read_text()).body
        version = next(ast.literal_eval(node.value) for node in nodes
                       if isinstance(node, ast.Assign) and any(
                           isinstance(target, ast.Name) and target.id == '__version__'
                           for target in node.targets))
        if version != SUPPORTED_VERSION:
            errors.append(f'version.py: expected {SUPPORTED_VERSION}, got {version!r}')
    except (OSError, SyntaxError, ValueError, TypeError, StopIteration):
        errors.append('version.py: unable to verify upstream version')
    for name, expected in CORE_SHA256.items():
        try:
            if hashlib.sha256((root / name).read_bytes()).hexdigest() != expected:
                errors.append(name + ': upstream source differs')
        except OSError:
            errors.append(name + ': unable to verify upstream source')
    return errors


def require_supported_core():
    errors = compatibility_errors()
    if errors:
        reason = '; '.join(errors)
        logger.error('m3u_slot guard refused incompatible core: %s', reason)
        raise RuntimeError('M3U reservation guard refused incompatible core: ' + reason)


def _report_active(guard):
    logger.info('m3u_slot guard active worker=%s compatible_core=%s upstream=%s '
                'reclamation_enabled=%s grace_seconds=%s source=%s', worker_id(),
                SUPPORTED_VERSION, UPSTREAM_REVISION, guard.reclamation_enabled,
                guard.module.GRACE_SECONDS, guard.module.__file__)
    return guard

_ACQUIRE = """
local held = redis.call('HGET', KEYS[1], 'state')
if held == 'held' then return {0, tonumber(redis.call('GET', KEYS[2]) or '0'), 'owner_busy'} end
local count = tonumber(redis.call('GET', KEYS[2]) or '0')
local limit = tonumber(ARGV[1])
if limit > 0 and count >= limit then return {0, count, 'profile_full'} end
if KEYS[3] ~= '' and tonumber(redis.call('GET', KEYS[3]) or '0') >= limit then
  return {0, count, 'credential_full'}
end
if limit > 0 then count = redis.call('INCR', KEYS[2]) end
if KEYS[3] ~= '' then
  redis.call('INCR', KEYS[3]); redis.call('SET', KEYS[4], KEYS[3])
end
local now = redis.call('TIME'); local stamp = tonumber(now[1]) + tonumber(now[2])/1000000
redis.call('DEL', KEYS[1])
redis.call('HSET', KEYS[1], 'state', 'held', 'profile', ARGV[2], 'counted', limit > 0 and '1' or '0',
 'credential', KEYS[3], 'owner', ARGV[3], 'client_id', ARGV[4], 'worker', ARGV[5],
 'kind', ARGV[6], 'channel', ARGV[7], 'assignment', ARGV[8], 'created', stamp, 'token', ARGV[9], 'liveness', ARGV[10])
redis.call('ZADD', KEYS[5], stamp, KEYS[1])
return {1, count, ''}
"""

# Both normal release and reclamation use the same single-winner transaction.
_RELEASE = """
if redis.call('HGET', KEYS[1], 'state') ~= 'held' then return 0 end
if ARGV[1] ~= '' and redis.call('HGET', KEYS[1], 'token') ~= ARGV[1] then return 0 end
if ARGV[2] == 'reclaim' then
 local created = tonumber(redis.call('HGET', KEYS[1], 'created') or '0')
 if tonumber(ARGV[3]) - created < tonumber(ARGV[4]) then return 0 end
 -- The assignment lock also covers requests that have not published metadata yet.
 if redis.call('EXISTS', KEYS[3]) > 0 then return 0 end
 if ARGV[5] == 'channel' then
  if redis.call('EXISTS', KEYS[4]) > 0 or redis.call('EXISTS', KEYS[5]) > 0
   or redis.call('SCARD', KEYS[6]) > 0 then return 0 end
  for i=7,#KEYS do if redis.call('EXISTS', KEYS[i]) > 0 then return 0 end end
 elseif ARGV[5] == 'task' then
  if redis.call('EXISTS', KEYS[4]) > 0 then return 0 end
 else return 0 end
end
local profile = redis.call('HGET', KEYS[1], 'profile')
local counter = 'profile_connections:' .. profile
if redis.call('HGET', KEYS[1], 'counted') == '1' and tonumber(redis.call('GET', counter) or '0') > 0 then
 redis.call('DECR', counter)
end
local credential = redis.call('HGET', KEYS[1], 'credential')
if credential and credential ~= '' and tonumber(redis.call('GET', credential) or '0') > 0 then
 redis.call('DECR', credential)
end
if tonumber(redis.call('GET', counter) or '0') <= 0 then redis.call('DEL', 'profile_credential_release:' .. profile) end
if ARGV[2] == 'reclaim' then
 local assignment = redis.call('HGET', KEYS[1], 'assignment')
 if assignment and assignment ~= '' then redis.call('DEL', assignment) end
end
redis.call('ZREM', KEYS[2], KEYS[1])
-- Keep a tombstone: a late finally must not decrement a different owner's slot.
redis.call('HSET', KEYS[1], 'state', 'released')
redis.call('EXPIRE', KEYS[1], 86400)
return 1
"""

_MOVE = """
if redis.call('HGET', KEYS[1], 'state') ~= 'held' then return 0 end
if redis.call('HGET', KEYS[1], 'profile') ~= ARGV[1] then return 0 end
local oldcred = redis.call('HGET', KEYS[1], 'credential') or ''
local limit = tonumber(ARGV[3]); local count = tonumber(redis.call('GET', KEYS[3]) or '0')
if limit > 0 and count >= limit then return 0 end
if KEYS[4] ~= oldcred and KEYS[4] ~= '' and tonumber(redis.call('GET', KEYS[4]) or '0') >= limit then return 0 end
if redis.call('HGET', KEYS[1], 'counted') == '1' and tonumber(redis.call('GET', KEYS[2]) or '0') > 0 then redis.call('DECR', KEYS[2]) end
if limit > 0 then redis.call('INCR', KEYS[3]) end
if oldcred ~= KEYS[4] then
 if oldcred ~= '' and tonumber(redis.call('GET', oldcred) or '0') > 0 then redis.call('DECR', oldcred) end
 if KEYS[4] ~= '' then redis.call('INCR', KEYS[4]) end
end
if KEYS[4] ~= '' then redis.call('SET', 'profile_credential_release:' .. ARGV[2], KEYS[4]) end
if tonumber(redis.call('GET', KEYS[2]) or '0') <= 0 then redis.call('DEL', 'profile_credential_release:' .. ARGV[1]) end
redis.call('HSET', KEYS[1], 'profile', ARGV[2], 'credential', KEYS[4], 'counted', limit > 0 and '1' or '0')
redis.call('SET', KEYS[5], ARGV[2])
if redis.call('HEXISTS', KEYS[6], 'm3u_profile') > 0 then redis.call('HSET', KEYS[6], 'm3u_profile', ARGV[2]) end
return 1
"""


def _str(value):
    return value.decode() if isinstance(value, bytes) else str(value or '')


def worker_id():
    return f'{socket.gethostname()}:{os.getpid()}'


def owner_key(owner):
    return 'm3u:slot:' + hashlib.sha256(owner.encode()).hexdigest()


def _lock_key(owner):
    return owner_key(owner) + ':assign'


@contextmanager
def scope(owner, *, client_id='', kind='channel', channel='', assignment='', liveness_key='', expected_token=None):
    previous = _scope.get() or {}
    token = _scope.set(dict(owner=owner, client_id=client_id or previous.get('client_id', ''),
                           kind=kind, channel=str(channel), assignment=assignment, liveness_key=liveness_key,
                           expected_token=expected_token if expected_token is not None else previous.get('expected_token')))
    try:
        yield
    finally:
        _scope.reset(token)


def _identity():
    return _scope.get() or dict(owner=f'unbound:{worker_id()}',
                               client_id='unbound', kind='unbound', channel='', assignment='', liveness_key='')


def _credential(profile, pool):
    group = pool.get_enforced_server_group_for_profile(profile)
    if not group or profile.max_streams == 0:
        return ''
    return pool._credential_counter_key(profile, group)


def _event(action, record, **extra):
    # Only IDs and numeric counters. Never include URLs, names, headers or credentials.
    logger.info('m3u_slot action=%s profile=%s owner=%s client_id=%s worker=%s token=%s %s',
                action, record.get('profile', ''), record.get('owner', ''),
                record.get('client_id', ''), record.get('worker', ''), record.get('token', ''), extra)


class Guard:
    def __init__(self, pool):
        self.pool = pool
        self.scope_var = _scope
        self.lock_var = _held_locks
        self.module = sys.modules[__name__]
        self.reclaim_offset = 0
        self.legacy_reserve = pool.reserve_profile_slot
        self.legacy_release = pool.release_profile_slot
        self.originals = []
        self.reclamation_enabled = True

    def reserve(self, profile, redis_client):
        ident = _identity()
        if ident['kind'] not in ('channel', 'task'):
            result = self.legacy_reserve(profile, redis_client)
            _event('legacy_acquire', dict(profile=profile.id, worker=worker_id(), **ident), reserved=result[0], count=result[1])
            return result
        # Resolve ORM/credential failures BEFORE changing any counter.
        credential = _credential(profile, self.pool)
        if credential is None:
            return False, int(redis_client.get(self.pool.profile_connections_key(profile.id)) or 0), 'credential_full'
        token = uuid.uuid4().hex
        result = redis_client.eval(_ACQUIRE, 5, owner_key(ident['owner']),
                                   self.pool.profile_connections_key(profile.id), credential,
                                   self.pool.profile_credential_release_key(profile.id), INDEX,
                                   profile.max_streams, profile.id, ident['owner'], ident['client_id'],
                                   worker_id(), ident['kind'], ident['channel'], ident['assignment'], token, ident['liveness_key'])
        reserved, count, failure = int(result[0]), int(result[1]), _str(result[2])
        if reserved:
            ident['expected_token'] = token
            _event('acquire', dict(profile=profile.id, worker=worker_id(), token=token, **ident), count=count)
        return bool(reserved), count, failure or None

    def release(self, profile_id, redis_client):
        ident = _identity()
        key = owner_key(ident['owner'])
        record = {_str(k): _str(v) for k, v in redis_client.hgetall(key).items()}
        if not record:
            # Streams already running when the guard was installed have no lease.
            released = self.legacy_release(profile_id, redis_client)
            _event('legacy_release', dict(profile=profile_id, worker=worker_id(), **ident), released=released)
            return released
        if record.get('state') == 'released':
            return True
        expected = ident.get('expected_token')
        if expected is not None and expected != record['token']:
            _event('stale_release_ignored', record, cleanup_client_id=ident.get('client_id', ''))
            return True
        released = redis_client.eval(_RELEASE, 2, key, INDEX, record['token'], 'release', 0, 0, '')
        if released:
            _event('release', record, requested_profile=profile_id)
        return True

    def reclaim(self, redis_client, *, now=None, grace=GRACE_SECONDS):
        if not self.reclamation_enabled:
            return 0
        now = time.time() if now is None else now
        reclaimed = 0
        candidates = redis_client.zrangebyscore(INDEX, '-inf', now - grace, start=self.reclaim_offset, num=200)
        self.reclaim_offset = self.reclaim_offset + len(candidates) if len(candidates) == 200 else 0
        for key in candidates:
            key = _str(key)
            record = {_str(k): _str(v) for k, v in redis_client.hgetall(key).items()}
            if record.get('state') != 'held':
                continue
            owner, kind = record['owner'], record['kind']
            keys = [key, INDEX, _lock_key(owner)]
            if kind == 'channel':
                channel = record['channel']
                keys += [f'live:channel:{channel}:metadata', f'live:channel:{channel}:owner',
                         f'live:channel:{channel}:clients']
                # Protect live client hashes even if a set membership was lost.
                keys += [_str(k) for k in redis_client.scan_iter(f'live:channel:{channel}:clients:*')]
            elif kind == 'task':
                if not record.get('liveness'):
                    continue
                keys += [record['liveness']]
            else:
                continue
            result = redis_client.eval(_RELEASE, len(keys), *keys, record['token'], 'reclaim', now, grace, kind)
            if result:
                _event('reclaim', record, age=round(now - float(record['created'])))
                reclaimed += 1
        return reclaimed

    def replace(self, obj, name, replacement):
        original = getattr(obj, name)
        self.originals.append((obj, name, original))
        setattr(obj, name, replacement)


def _stale_cleanup(redis_client, owner):
    expected = _identity().get('expected_token')
    record = {_str(k): _str(v) for k, v in redis_client.hgetall(owner_key(owner)).items()}
    if expected is not None and record.get('state') == 'held' and expected != record.get('token'):
        _event('stale_cleanup_ignored', record, cleanup_client_id=_identity().get('client_id', ''))
        return True
    return False


@contextmanager
def _assignment_lock(redis_client, owner):
    held = _held_locks.get()
    if owner in held:
        yield True
        return
    lock = redis_client.lock(_lock_key(owner), timeout=180, blocking_timeout=5)
    if not lock.acquire():
        yield False
        return
    token = _held_locks.set(held | {owner})
    try:
        yield True
    finally:
        _held_locks.reset(token)
        lock.release()


def _model_wrapper(original, *, preview=False, release=False):
    signature = inspect.signature(original)
    @wraps(original)
    def wrapped(self, *args, **kwargs):
        from core.utils import RedisClient
        bound = signature.bind(self, *args, **kwargs)
        profile = bound.arguments.get('preferred_profile_id', bound.arguments.get('m3u_profile_id'))
        if preview:
            channel = f'{self.stream_hash}.p{profile}' if profile is not None else self.stream_hash
            assignment = f'stream_profile:{self.id}:p{profile}' if profile is not None else self.preview_profile_key(self.id)
        else:
            channel = str(self.uuid)
            assignment = f'channel_stream:{self.id}'
        owner = 'channel:' + channel
        redis_client = RedisClient.get_client()
        # One lock spans lookup, reserve and publication, including reconnects
        # that bypass the proxy's initialization ownership gate.
        with _assignment_lock(redis_client, owner) as acquired:
            if not acquired:
                return False if release else (None, None, 'Reservation busy, retry', False)
            if release and _stale_cleanup(redis_client, owner):
                return False
            with scope(owner, channel=channel, assignment=assignment):
                return original(self, *args, **kwargs)
    return wrapped


def _scoped(original, identity):
    signature = inspect.signature(original)
    @wraps(original)
    def wrapped(*args, **kwargs):
        values = signature.bind(*args, **kwargs).arguments
        ident = identity(values)
        with scope(**ident):
            return original(*args, **kwargs)
    return wrapped


def install():
    """Install once per web worker, preserving imported aliases across reloads."""
    # Check before even reusing an installed guard: a loader reload must never
    # silently accept an upstream update with incompatible lifecycle semantics.
    require_supported_core()
    from apps.m3u import connection_pool as pool
    guard = Guard(pool)
    # 0.34's plugin-leader-election OS thread discovers plugins while app-ready
    # discovery runs, so two installs can start at once in one worker. One
    # atomic claim picks the installer; DO NOT test-then-set, which lets both
    # wrap every hook twice.
    installed = sys.modules.get(_REGISTRY) or sys.modules.setdefault(_REGISTRY, guard)
    if installed is not guard:
        installed.reclamation_enabled = True
        return _report_active(installed)
    try:
        _patch_core(guard)
    except BaseException:
        for obj, name, original in reversed(guard.originals):
            setattr(obj, name, original)
        sys.modules.pop(_REGISTRY, None)
        raise
    from core.utils import RedisClient
    RedisClient.get_client().hset(WORKERS, worker_id(), VERSION)
    logger.info('m3u_slot guard installed worker=%s version=%s', worker_id(), VERSION)
    return _report_active(guard)


def _patch_core(guard):
    from apps.m3u import connection_pool as pool
    from apps.channels import models
    from apps.proxy.live_proxy import client_manager, slot_release, views, server
    from core.utils import RedisClient
    reserve, release = guard.reserve, guard.release
    for obj, name, function in [(pool, 'reserve_profile_slot', reserve), (models, 'reserve_profile_slot', reserve),
                                (pool, 'release_profile_slot', release), (models, 'release_profile_slot', release),
                                (slot_release, 'release_profile_slot', release)]:
        guard.replace(obj, name, function)
    # This module imports pool aliases at module load; later imports naturally
    # receive the wrapped functions. Preserve catch-up's existing session pairing.
    timeshift = sys.modules.get('apps.timeshift.views')
    if timeshift is not None:
        guard.replace(timeshift, 'reserve_profile_slot', reserve)
        guard.replace(timeshift, 'release_profile_slot', release)
    for cls in (models.Channel, models.Stream):
        for name in ('get_stream', 'release_stream'):
            guard.replace(cls, name, _model_wrapper(getattr(cls, name), preview=cls is models.Stream,
                                                   release=name == 'release_stream'))
    original_move = models.Channel.update_stream_profile
    @wraps(original_move)
    def move(channel_obj, new_profile_id):
        redis_client = RedisClient.get_client()
        owner = 'channel:' + str(channel_obj.uuid)
        lock = redis_client.lock(_lock_key(owner), timeout=180, blocking_timeout=5)
        if not lock.acquire():
            return False
        try:
            record = {_str(k): _str(v) for k, v in redis_client.hgetall(owner_key(owner)).items()}
            if record.get('state') != 'held':
                return original_move(channel_obj, new_profile_id)
            old_profile = int(record['profile'])
            if old_profile == int(new_profile_id):
                return True
            stream_id = redis_client.get(f'channel_stream:{channel_obj.id}')
            if not stream_id:
                return False
            from apps.m3u.models import M3UAccountProfile
            profile = M3UAccountProfile.objects.select_related('m3u_account__server_group').get(pk=new_profile_id)
            credential = _credential(profile, pool)
            if credential is None:
                return False
            result = redis_client.eval(_MOVE, 6, owner_key(owner), pool.profile_connections_key(old_profile),
                pool.profile_connections_key(new_profile_id), credential,
                f'stream_profile:{_str(stream_id)}', f'live:channel:{channel_obj.uuid}:metadata',
                old_profile, new_profile_id, profile.max_streams)
            if result:
                _event('move', dict(record, profile=new_profile_id), previous_profile=old_profile)
            return bool(result)
        finally:
            lock.release()
    guard.replace(models.Channel, 'update_stream_profile', move)
    guard.replace(views, '_obtain_stream_with_retries', _scoped(views._obtain_stream_with_retries,
        lambda v: dict(owner='channel:' + v['channel_id'], channel=v['channel_id'], client_id=v['client_id'])))
    guard.replace(client_manager.ClientManager, 'add_client', _scoped(client_manager.ClientManager.add_client,
        lambda v: dict(owner='channel:' + v['self'].channel_id, channel=v['self'].channel_id, client_id=v['client_id'])))
    original_claim = slot_release.claim_and_release_profile_slot
    @wraps(original_claim)
    def claim(redis_client, metadata_key, *args, **kwargs):
        if metadata_key and metadata_key.startswith('live:channel:') and metadata_key.endswith(':metadata'):
            channel = metadata_key[len('live:channel:'):-len(':metadata')]
            owner = 'channel:' + channel
            with _assignment_lock(redis_client, owner) as acquired:
                if not acquired or _stale_cleanup(redis_client, owner):
                    return False
                with scope(owner, channel=channel):
                    return original_claim(redis_client, metadata_key, *args, **kwargs)
        return original_claim(redis_client, metadata_key, *args, **kwargs)
    guard.replace(slot_release, 'claim_and_release_profile_slot', claim)
    guard.replace(models, 'claim_and_release_profile_slot', claim)
    from apps.proxy.live_proxy.output.ts.generator import StreamGenerator
    from apps.proxy.live_proxy.output.fmp4.generator import FMP4StreamGenerator
    for cls in (StreamGenerator, FMP4StreamGenerator):
        original_init = cls.__init__
        def make_init(original):
            @wraps(original)
            def init(generator, *args, **kwargs):
                original(generator, *args, **kwargs)
                generator._m3u_slot_token = _str(RedisClient.get_client().hget(
                    owner_key('channel:' + generator.channel_id), 'token'))
            return init
        guard.replace(cls, '__init__', make_init(original_init))
        guard.replace(cls, '_cleanup', _scoped(cls._cleanup, lambda v: dict(
            owner='channel:' + v['self'].channel_id, channel=v['self'].channel_id,
            client_id=v['self'].client_id, expected_token=getattr(v['self'], '_m3u_slot_token', ''))))
    original_cleanup = server.ProxyServer._check_orphaned_metadata
    @wraps(original_cleanup)
    def cleanup(proxy):
        original_cleanup(proxy)
        try:
            guard.reclaim(proxy.redis_client)
        except Exception:
            logger.exception('m3u_slot reclaim failed; no forced counter reset')
    guard.replace(server.ProxyServer, '_check_orphaned_metadata', cleanup)
