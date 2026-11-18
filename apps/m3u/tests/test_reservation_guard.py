"""Real-Redis regression tests; opt in only on an isolated test Redis."""
import os
import importlib.util
import sys
import threading
import time
from types import SimpleNamespace as N
from unittest import SkipTest
from unittest.mock import patch

import redis
from django.test import SimpleTestCase

from apps.channels.models import Channel
from apps.m3u import connection_pool as pool
from apps.m3u import reservation_guard as rg
from apps.proxy.live_proxy.constants import ChannelMetadataField


class Query(list):
    def all(self):
        return self

    def filter(self, **kwargs):
        return self

    def order_by(self, *args):
        return self

    def exists(self):
        return bool(self)


class ReservationGuardTests(SimpleTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        url = os.environ.get('RESERVATION_GUARD_TEST_REDIS')
        if not url:
            raise SkipTest('Requires explicitly isolated RESERVATION_GUARD_TEST_REDIS')
        cls.redis = redis.Redis.from_url(url)
        cls.redis.ping()

    def setUp(self):
        # The opt-in URL must name a disposable Redis database.
        self.redis.flushdb()
        self.guard = rg.Guard(pool)
        self.profile = N(id=4, pk=4, max_streams=4, name='scratch', is_default=True,
                         is_active=True, m3u_account=N(id=1, is_active=True, server_group=None))
        self.profile.m3u_account.profiles = Query([self.profile])

    def tearDown(self):
        self.redis.flushdb()

    def reserve(self, owner='channel:test', **kwargs):
        with rg.scope(owner, client_id='client:test', channel='test', **kwargs):
            return self.guard.reserve(self.profile, self.redis)

    def age(self, owner='channel:test'):
        key = rg.owner_key(owner)
        self.redis.hset(key, 'created', time.time() - 600)
        self.redis.zadd(rg.INDEX, {key: time.time() - 600})

    def test_owner_cannot_reserve_twice_and_release_is_single_winner(self):
        self.assertTrue(self.reserve()[0])
        self.assertFalse(self.reserve()[0])
        self.assertEqual(int(self.redis.get('profile_connections:4')), 1)
        with rg.scope('channel:test'):
            self.assertTrue(self.guard.release(4, self.redis))
            self.assertTrue(self.guard.release(4, self.redis))
        self.assertEqual(int(self.redis.get('profile_connections:4')), 0)

    def test_different_owners_respect_capacity(self):
        for i in range(4):
            self.assertTrue(self.reserve('channel:' + str(i))[0])
        self.assertEqual(self.reserve('channel:full'), (False, 4, 'profile_full'))
        self.assertEqual(self.redis.zcard(rg.INDEX), 4)

    def test_unlimited_profile_never_increments_or_decrements_an_existing_count(self):
        self.profile.max_streams = 0
        self.redis.set('profile_connections:4', 3)
        self.assertTrue(self.reserve()[0])
        with rg.scope('channel:test'):
            self.guard.release(4, self.redis)
        self.assertEqual(int(self.redis.get('profile_connections:4')), 3)

    def test_credential_resolution_error_never_leaves_a_counter(self):
        with patch.object(pool, 'get_enforced_server_group_for_profile', side_effect=RuntimeError('scratch')):
            with self.assertRaises(RuntimeError):
                self.reserve()
        self.assertIsNone(self.redis.get('profile_connections:4'))
        self.assertEqual(self.redis.zcard(rg.INDEX), 0)

    def test_missing_credential_rejects_before_profile_increment(self):
        with patch.object(pool, 'get_enforced_server_group_for_profile', return_value=object()), \
             patch.object(pool, '_credential_counter_key', return_value=None):
            self.assertEqual(self.reserve(), (False, 0, 'credential_full'))
        self.assertIsNone(self.redis.get('profile_connections:4'))

    def test_credential_pool_shared_across_profiles_and_retained_until_last_release(self):
        with patch.object(pool, 'get_enforced_server_group_for_profile', return_value=object()), \
             patch.object(pool, '_credential_counter_key', return_value='server_group_connections:scratch'):
            self.assertTrue(self.reserve()[0])
            self.profile.id = 5
            self.assertTrue(self.reserve('channel:second')[0])
            self.assertEqual(int(self.redis.get('server_group_connections:scratch')), 2)
            with rg.scope('channel:test'):
                self.guard.release(4, self.redis)
            self.assertEqual(int(self.redis.get('server_group_connections:scratch')), 1)
            with rg.scope('channel:second'):
                self.guard.release(5, self.redis)
            self.assertEqual(int(self.redis.get('server_group_connections:scratch')), 0)

    def test_abandoned_channel_reclaimed_without_touching_other_owner(self):
        self.assertTrue(self.reserve(assignment='channel_stream:1')[0])
        self.redis.set('channel_stream:1', 10)
        self.redis.set('stream_profile:10', 4)
        with rg.scope('channel:other', client_id='other-client', channel='other'):
            self.assertTrue(self.guard.reserve(self.profile, self.redis)[0])
        self.age()
        self.assertEqual(self.guard.reclaim(self.redis), 1)
        self.assertEqual(int(self.redis.get('profile_connections:4')), 1)
        self.assertIsNone(self.redis.get('channel_stream:1'))
        # A stream may be shared by another channel: never remove its bare key.
        self.assertEqual(self.redis.get('stream_profile:10'), b'4')
        with rg.scope('channel:test'):
            self.guard.release(4, self.redis)
        self.assertEqual(int(self.redis.get('profile_connections:4')), 1)

    def test_every_liveness_signal_independently_prevents_reclaim(self):
        makers = [
            lambda: self.redis.set(rg._lock_key('channel:test'), 'assigning'),
            lambda: self.redis.hset('live:channel:test:metadata', mapping={'state': 'active'}),
            lambda: self.redis.set('live:channel:test:owner', 'worker'),
            lambda: self.redis.sadd('live:channel:test:clients', 'viewer'),
            lambda: self.redis.hset('live:channel:test:clients:viewer', mapping={'last_active': time.time()}),
        ]
        for maker in makers:
            with self.subTest(maker=maker):
                self.redis.flushdb()
                self.assertTrue(self.reserve()[0])
                self.age()
                maker()
                self.assertEqual(self.guard.reclaim(self.redis), 0)
                self.assertEqual(int(self.redis.get('profile_connections:4')), 1)

    def test_new_reservation_has_grace_and_disabled_reclaimer_does_nothing(self):
        self.reserve()
        self.assertEqual(self.guard.reclaim(self.redis), 0)
        self.age()
        self.guard.reclamation_enabled = False
        self.assertEqual(self.guard.reclaim(self.redis), 0)

    def test_probe_lease_reclaimed_only_after_its_global_probe_lock_is_gone(self):
        self.reserve('probe:test', kind='task', liveness_key='probe:global')
        self.age('probe:test')
        self.redis.set('probe:global', 'busy')
        self.assertEqual(self.guard.reclaim(self.redis), 0)
        self.redis.delete('probe:global')
        self.assertEqual(self.guard.reclaim(self.redis), 1)

    def test_probe_finally_retains_its_acquired_generation_after_replacement(self):
        with rg.scope('probe:test', kind='task', liveness_key='probe:global'):
            self.assertTrue(self.guard.reserve(self.profile, self.redis)[0])
            self.age('probe:test')
            self.assertEqual(self.guard.reclaim(self.redis), 1)
            with rg.scope('probe:test', kind='task', liveness_key='probe:global'):
                self.assertTrue(self.guard.reserve(self.profile, self.redis)[0])
            self.assertTrue(self.guard.release(4, self.redis))
        self.assertEqual(int(self.redis.get('profile_connections:4')), 1)

    def test_release_reclaim_race_decrements_once(self):
        self.reserve()
        self.age()
        barrier = threading.Barrier(2)
        def release():
            barrier.wait(5)
            with rg.scope('channel:test'):
                self.guard.release(4, self.redis)
        thread = threading.Thread(target=release)
        thread.start()
        barrier.wait(5)
        self.guard.reclaim(self.redis)
        thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(int(self.redis.get('profile_connections:4')), 0)

    def test_legacy_running_stream_releases_through_original_pool(self):
        self.redis.set('profile_connections:4', 1)
        with rg.scope('channel:already-running'):
            self.assertTrue(self.guard.release(4, self.redis))
        self.assertEqual(int(self.redis.get('profile_connections:4')), 0)

    def test_late_old_client_cannot_release_replacement_generation(self):
        self.reserve(assignment='channel_stream:1')
        old_token = self.redis.hget(rg.owner_key('channel:test'), 'token').decode()
        self.age()
        self.assertEqual(self.guard.reclaim(self.redis), 1)
        self.reserve(assignment='channel_stream:1')
        self.redis.set('channel_stream:1', 10)
        self.redis.set('stream_profile:10', 4)
        self.redis.hset('live:channel:test:metadata', mapping={'stream_id': 10, 'm3u_profile': 4})
        channel = N(id=1, uuid='test')
        with rg.scope('channel:test', client_id='old-client', expected_token=old_token), \
             patch('core.utils.RedisClient.get_client', return_value=self.redis):
            self.assertTrue(self.guard.release(4, self.redis))
            self.assertFalse(rg._model_wrapper(Channel.release_stream, release=True)(channel))
        self.assertEqual(int(self.redis.get('profile_connections:4')), 1)
        self.assertEqual(self.redis.get('channel_stream:1'), b'10')
        self.assertEqual(self.redis.hget('live:channel:test:metadata', 'm3u_profile'), b'4')

    def test_more_than_200_live_owners_do_not_starve_later_abandoned_owner(self):
        self.profile.max_streams = 0
        for i in range(201):
            owner = 'channel:' + str(i)
            with rg.scope(owner, channel=str(i)):
                self.guard.reserve(self.profile, self.redis)
            self.age(owner)
            if i < 200:
                self.redis.hset(f'live:channel:{i}:metadata', mapping={'state': 'active'})
        self.assertEqual(self.guard.reclaim(self.redis), 0)
        self.assertEqual(self.guard.reclaim(self.redis), 1)
        self.assertEqual(self.redis.hget(rg.owner_key('channel:200'), 'state'), b'released')

    def test_unbound_vod_and_timeshift_callers_preserve_stock_pairing(self):
        self.assertTrue(self.guard.reserve(self.profile, self.redis)[0])
        self.assertTrue(self.guard.release(4, self.redis))
        self.assertEqual(self.redis.zcard(rg.INDEX), 0)
        self.assertEqual(int(self.redis.get('profile_connections:4')), 0)

    def test_logs_include_owner_client_and_profile_without_provider_credentials(self):
        with self.assertLogs(rg.logger, level='INFO') as logs:
            self.reserve()
            with rg.scope('channel:test'):
                self.guard.release(4, self.redis)
        text = '\n'.join(logs.output)
        for value in ('action=acquire', 'action=release', 'client_id=client:test', 'owner=channel:test', 'profile=4'):
            self.assertIn(value, text)

    def test_concurrent_actual_channel_get_stream_keeps_one_assignment_one_slot(self):
        """Actual deployed get_stream body, two workers, a paused first reserve."""
        stream = N(id=10, m3u_account=self.profile.m3u_account)
        channel = N(id=1, uuid='test', streams=Query([stream]))
        channel._stream_assignment_is_reusable = lambda r, s: True
        first_entered, second_entered, let_first_finish = (threading.Event() for _ in range(3))
        results, errors = [], []
        calls = []
        def reserve(profile, client):
            calls.append(threading.get_ident())
            if len(calls) == 1:
                first_entered.set()
                if not let_first_finish.wait(5):
                    raise AssertionError('First worker timed out')
            else:
                second_entered.set()
            return (pool.reserve_profile_slot if os.environ.get('W466_BROKEN') else self.guard.reserve)(profile, client)
        original = Channel.get_stream
        get_stream = original if os.environ.get('W466_BROKEN') else rg._model_wrapper(original)
        def reconnect():
            try:
                with rg.scope('channel:test', client_id='reconnected-viewer', channel='test'):
                    results.append(get_stream(channel))
            except BaseException as exc:
                errors.append(exc)
        with patch('core.utils.RedisClient.get_client', return_value=self.redis), \
             patch('apps.channels.models.reserve_profile_slot', side_effect=reserve):
            first = threading.Thread(target=reconnect)
            second = threading.Thread(target=reconnect)
            first.start()
            self.assertTrue(first_entered.wait(5))
            second.start()
            # This event fires on the broken body; fixed code waits on Redis lock.
            second_entered.wait(0.2)
            let_first_finish.set()
            first.join(6)
            second.join(6)
        self.assertEqual(errors, [])
        self.assertFalse(first.is_alive() or second.is_alive())
        self.assertEqual(len(calls), 1, 'A second worker attempted another reservation for the same channel')
        self.assertEqual(len(results), 2)
        self.assertEqual(sum(result[3] for result in results), 1)
        self.assertEqual(int(self.redis.get('profile_connections:4')), 1)
        with patch('core.utils.RedisClient.get_client', return_value=self.redis), \
             patch('apps.proxy.live_proxy.slot_release.release_profile_slot', side_effect=self.guard.release):
            release = rg._model_wrapper(Channel.release_stream, release=True)
            self.assertTrue(release(channel))
            self.assertFalse(release(channel))
        self.assertEqual(int(self.redis.get('profile_connections:4')), 0)

    def test_profile_move_updates_owned_release_atomically_and_obeys_capacity(self):
        self.reserve()
        key = rg.owner_key('channel:test')
        args = (6, key, 'profile_connections:4', 'profile_connections:5', '',
                'stream_profile:10', 'live:channel:test:metadata', 4, 5, 1)
        self.redis.set('profile_connections:5', 1)
        self.assertEqual(self.redis.eval(rg._MOVE, *args), 0)
        self.redis.set('profile_connections:5', 0)
        self.redis.hset('live:channel:test:metadata', 'm3u_profile', 4)
        self.assertEqual(self.redis.eval(rg._MOVE, *args), 1)
        self.assertEqual(self.redis.hget(key, 'profile'), b'5')
        self.assertEqual(self.redis.hget('live:channel:test:metadata', 'm3u_profile'), b'5')
        with rg.scope('channel:test'):
            self.guard.release(5, self.redis)
        self.assertEqual(int(self.redis.get('profile_connections:4')), 0)
        self.assertEqual(int(self.redis.get('profile_connections:5')), 0)

    def test_install_and_reload_patch_real_deployed_aliases_without_new_threads(self):
        before = {t.ident for t in threading.enumerate()}
        from apps.channels import models
        from apps.proxy.live_proxy import client_manager, slot_release
        from apps.timeshift import views as timeshift
        try:
            with patch('core.utils.RedisClient.get_client', return_value=self.redis):
                guard = rg.install()
                self.assertIs(rg.install(), guard)
            self.assertEqual({t.ident for t in threading.enumerate()}, before)
            self.assertEqual(models.reserve_profile_slot, guard.reserve)
            self.assertEqual(slot_release.release_profile_slot, guard.release)
            self.assertEqual(timeshift.reserve_profile_slot, guard.reserve)
            self.assertEqual(timeshift.release_profile_slot, guard.release)
            self.assertTrue(hasattr(client_manager.ClientManager.add_client, '__wrapped__'))
            self.assertEqual(self.redis.hget(rg.WORKERS, rg.worker_id()), rg.VERSION.encode())
            # Replay loader eviction and re-import under another package name.
            name = '_w466_reloaded_guard'
            spec = importlib.util.spec_from_file_location(name, rg.__file__)
            module = importlib.util.module_from_spec(spec)
            sys.modules[name] = module
            try:
                spec.loader.exec_module(module)
                self.assertIs(module.install(), guard)
                with module.scope('probe:after-reload', kind='task', client_id='probe-client', liveness_key='probe:lock'):
                    self.assertTrue(guard.reserve(self.profile, self.redis)[0])
                record = self.redis.hgetall(rg.owner_key('probe:after-reload'))
                self.assertEqual(record[b'client_id'], b'probe-client')
                self.assertEqual(record[b'kind'], b'task')
            finally:
                sys.modules.pop(name, None)
        finally:
            guard = sys.modules.pop(rg._REGISTRY, None)
            if guard:
                for obj, name, original in reversed(guard.originals):
                    setattr(obj, name, original)

    def _undo_installs(self, *guards):
        from apps.channels import models
        registered = sys.modules.pop(rg._REGISTRY, None)
        for guard in {id(g): g for g in (*guards, registered) if g}.values():
            for obj, name, original in reversed(guard.originals):
                setattr(obj, name, original)

    def _start_install(self, results, errors):
        def install():
            try:
                results.append(rg.install())
            except Exception as error:  # surfaced by the caller
                errors.append(error)
        worker = threading.Thread(target=install)
        worker.start()
        return worker

    def _second_installer_waits_inside_first(self, first_body):
        """Run two installs on real threads; the first holds the lock until the second waits.

        0.34's leader-election thread discovers plugins beside app-ready discovery.
        """
        holding, second_waiting = threading.Event(), threading.Event()
        acquire = rg._acquire_cooperatively
        patch_core = rg._patch_core
        calls, results, errors = [], [], []

        def acquire_noting_waiter(lock):
            if holding.is_set():
                # The holder waits for this event, so the lock is held here:
                # prove the second installer meets a contended lock.
                self.assertFalse(lock.acquire(blocking=False))
                second_waiting.set()
            return acquire(lock)

        def patch_core_holding(guard):
            calls.append(guard)
            if len(calls) == 1:
                holding.set()
                self.assertTrue(second_waiting.wait(5), 'second installer never reached the lock')
                first_body(guard, patch_core)
            else:
                patch_core(guard)

        with patch('core.utils.RedisClient.get_client', return_value=self.redis), \
             patch.object(rg, 'require_supported_core', lambda: None), \
             patch.object(rg, '_acquire_cooperatively', acquire_noting_waiter), \
             patch.object(rg, '_patch_core', patch_core_holding):
            first = self._start_install(results, errors)
            self.assertTrue(holding.wait(5))
            second = self._start_install(results, errors)
            first.join(10)
            second.join(10)
        return calls, results, errors

    def test_concurrent_first_installs_in_one_worker_patch_each_hook_once(self):
        from apps.channels import models
        original_move = models.Channel.update_stream_profile
        calls = []
        try:
            calls, results, errors = self._second_installer_waits_inside_first(
                lambda guard, patch_core: patch_core(guard))
            self.assertEqual(errors, [])
            self.assertEqual(len(results), 2)
            self.assertIs(results[0], results[1])
            self.assertEqual(len(calls), 1)
            self.assertIs(models.Channel.update_stream_profile.__wrapped__, original_move)
        finally:
            self._undo_installs(*calls)
            models.Channel.update_stream_profile = original_move

    def test_waiting_installer_installs_cleanly_after_the_first_rolls_back(self):
        from apps.channels import models
        original_reserve = pool.reserve_profile_slot
        original_move = models.Channel.update_stream_profile
        calls = []

        def fails_after_patching(guard, patch_core):
            patch_core(guard)
            raise RuntimeError('scratch failure after every hook was patched')

        try:
            calls, results, errors = self._second_installer_waits_inside_first(fails_after_patching)
            self.assertEqual(len(errors), 1)
            self.assertEqual(len(results), 1)
            installed = results[0]
            self.assertIs(sys.modules[rg._REGISTRY], installed)
            self.assertIs(installed, calls[1])
            self.assertIs(installed.legacy_reserve, original_reserve)
            self.assertEqual(pool.reserve_profile_slot, installed.reserve)
            self.assertIs(models.Channel.update_stream_profile.__wrapped__, original_move)
        finally:
            self._undo_installs(*calls)
            models.Channel.update_stream_profile = original_move

    def test_greenlet_waiter_yields_to_a_holder_on_the_same_hub(self):
        """A waiter that blocks in acquire() stops the hub, so the holder never resumes."""
        import gevent
        patch_core = rg._patch_core
        calls, results, errors, holder_pauses = [], [], [], []

        def holder_yields(guard):
            calls.append(guard)
            paused = time.monotonic()
            gevent.sleep(0.05)  # e.g. a contended import or logging lock
            # A waiter that blocks the hub delays this resume by however long it blocks.
            holder_pauses.append(time.monotonic() - paused)
            patch_core(guard)

        def install():
            try:
                results.append(rg.install())
            except Exception as error:  # surfaced below
                errors.append(error)

        try:
            with patch('core.utils.RedisClient.get_client', return_value=self.redis), \
                 patch.object(rg, 'require_supported_core', lambda: None), \
                 patch.object(rg, 'INSTALL_WAIT_SECONDS', 1, create=True), \
                 patch.object(rg, '_patch_core', holder_yields):
                gevent.joinall([gevent.spawn(install), gevent.spawn(install)], timeout=60)
            self.assertEqual(errors, [])
            self.assertEqual(len(results), 2)
            self.assertIs(results[0], results[1])
            self.assertEqual(len(calls), 1)
            self.assertLess(holder_pauses[0], 0.5, 'the waiting greenlet stalled the hub')
        finally:
            self._undo_installs(*calls)

    def test_guard_module_resolves_itself_when_evicted_during_import(self):
        name = '_w466_evicted_during_import'
        spec = importlib.util.spec_from_file_location(name, rg.__file__)
        module = importlib.util.module_from_spec(spec)
        # Never placed in sys.modules: the loader evicted it before _MODULE ran.
        spec.loader.exec_module(module)
        self.assertIs(module._MODULE, module)

    def test_failed_install_restores_hooks_and_publishes_nothing(self):
        from apps.channels import models
        original_get = models.Channel.get_stream
        with patch('core.utils.RedisClient.get_client', return_value=self.redis), \
             patch.object(rg, 'require_supported_core', lambda: None), \
             patch('apps.proxy.live_proxy.output.fmp4.generator.FMP4StreamGenerator._cleanup', None):
            # Fails late, after the pool, model and TS generator hooks are in place.
            with self.assertRaises(TypeError):
                rg.install()
        self.assertNotIn(rg._REGISTRY, sys.modules)
        self.assertIs(models.Channel.get_stream, original_get)
        self.assertIs(pool.reserve_profile_slot, self.guard.legacy_reserve)

    def test_worker_registration_failure_keeps_the_complete_install(self):
        failing = N(hset=lambda *args: (_ for _ in ()).throw(redis.ConnectionError('scratch')))
        try:
            with patch('core.utils.RedisClient.get_client', return_value=failing), \
                 patch.object(rg, 'require_supported_core', lambda: None), \
                 self.assertLogs('apps.m3u.reservation_guard', level='ERROR'):
                guard = rg.install()
            self.assertIs(sys.modules[rg._REGISTRY], guard)
            self.assertEqual(pool.reserve_profile_slot, guard.reserve)
        finally:
            self._undo_installs()

    def test_guard_keeps_its_module_after_loader_eviction(self):
        name = rg.__name__
        try:
            sys.modules.pop(name)
            self.assertIs(rg.Guard(pool).module, rg)
        finally:
            sys.modules[name] = rg

    def test_maintenance_reclaims_only_while_this_worker_holds_the_enabled_plugin(self):
        from apps.plugins.loader import PluginManager
        for loaded, expected in ((None, False), (N(instance=None), False), (N(instance=object()), True)):
            with self.subTest(loaded=loaded), \
                 patch.object(PluginManager, 'get', return_value=N(get_plugin={rg.PLUGIN_KEY: loaded}.get)):
                self.assertIs(rg._plugin_enabled_here(), expected)
        from apps.proxy.live_proxy import server
        stock_checks = []
        try:
            with patch('core.utils.RedisClient.get_client', return_value=self.redis), \
                 patch.object(rg, 'require_supported_core', lambda: None), \
                 patch.object(server.ProxyServer, '_check_orphaned_metadata', stock_checks.append):
                guard = rg.install()
                proxy = N(redis_client=self.redis)
                for enabled in (False, True):
                    with self.subTest(enabled=enabled), \
                         patch.object(rg, '_plugin_enabled_here', return_value=enabled), \
                         patch.object(guard, 'reclaim') as reclaim:
                        server.ProxyServer._check_orphaned_metadata(proxy)
                        self.assertEqual(reclaim.called, enabled)
            self.assertEqual(stock_checks, [proxy, proxy])
        finally:
            self._undo_installs()

    def test_release_never_takes_a_credential_slot_its_profile_no_longer_counts(self):
        with patch.object(pool, 'get_enforced_server_group_for_profile', return_value=object()), \
             patch.object(pool, '_credential_counter_key', return_value='server_group_connections:scratch'):
            self.assertTrue(self.reserve()[0])
            self.profile.id = 5
            self.assertTrue(self.reserve('channel:second')[0])
        # An unpaired stock release (the 0.34 VOD resume path) already took profile 4's slot.
        self.assertTrue(pool.release_profile_slot(4, self.redis))
        self.assertEqual(int(self.redis.get('server_group_connections:scratch')), 1)
        with rg.scope('channel:test'):
            self.guard.release(4, self.redis)
        # Profile 5's channel still holds the shared credential slot.
        self.assertEqual(int(self.redis.get('server_group_connections:scratch')), 1)
