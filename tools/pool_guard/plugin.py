"""No-restart adapter for the core reservation guard on Dispatcharr 0.34.1."""
import os
import sys


class Plugin:
    name = 'M3U Reservation Guard'
    version = '0.2.3'
    description = 'Tracks provider slot ownership and reclaims abandoned reservations.'
    actions = [{'id': 'status', 'label': 'Status'}]

    # DO NOT move this to on_leader_acquired. The guard is per-process hooks on
    # the proxy's slot accounting, not a background service: every uWSGI web
    # worker serving streams must carry them. A leader-elected service would
    # run in one process and leave the others unguarded. Install creates no
    # thread; reclamation rides the proxy's existing maintenance loop.
    def __init__(self):
        from apps.plugins.models import PluginConfig
        if not PluginConfig.objects.filter(key='m3u_pool_guard', enabled=True).exists():
            return
        # manage.py / Celery discover plugins too. Only web workers own proxy hooks.
        if 'uwsgi' not in os.path.basename(sys.argv[0]):
            return
        from . import reservation_guard
        guard = reservation_guard.install()
        sys.modules['apps.m3u.reservation_guard'] = guard.module

    def run(self, action=None, params=None, context=None):
        from . import reservation_guard
        guard = sys.modules.get('_dispatcharr_m3u_pool_guard')
        errors = reservation_guard.compatibility_errors()
        # Dispatcharr 0.34 runs every action on a Celery worker, which never
        # installs the guard (see __init__). There the web workers' own
        # registration is the evidence, not this process's sys.modules.
        workers = {} if guard else _web_workers(reservation_guard.WORKERS)
        installed = bool(guard or workers)
        versions = sorted(set(workers.values()))
        return {'status': 'ok' if installed and not errors else 'error', 'installed': installed,
                'compatible': not errors, 'compatibility_errors': errors,
                'core_version': reservation_guard.SUPPORTED_VERSION,
                'core_revision': reservation_guard.UPSTREAM_REVISION,
                # Held in each web worker's memory; unknown from another process.
                'reclamation_enabled': bool(guard.reclamation_enabled) if guard else None,
                'grace_seconds': guard.module.GRACE_SECONDS if guard else None,
                'source_file': guard.module.__file__ if guard else None,
                'guard_version': guard.module.VERSION if guard else (
                    versions[0] if len(versions) == 1 else versions or None),
                'web_workers': workers,
                'worker_pid': os.getpid(), 'version': self.version}

    def stop(self, context=None):
        # No threads are created. Keep ownership-aware release until in-flight
        # leases drain. This stops reclamation in the worker serving the
        # disable; peers stop once rediscovery leaves them a disabled
        # placeholder (reservation_guard._plugin_enabled_here).
        guard = sys.modules.get('_dispatcharr_m3u_pool_guard')
        if guard:
            guard.reclamation_enabled = False


_PROC = '/proc'


def _web_workers(registry_key):
    """Live web workers on this host that installed the guard, with the guard
    version each runs. install() records `<host>:<pid>` -> VERSION in Redis;
    an entry whose process is gone, or is not a uWSGI worker, is left out."""
    import socket
    from core.utils import RedisClient
    host = socket.gethostname()
    live = {}
    for raw_id, raw_version in RedisClient.get_client().hgetall(registry_key).items():
        worker = raw_id.decode() if isinstance(raw_id, bytes) else str(raw_id)
        version = raw_version.decode() if isinstance(raw_version, bytes) else str(raw_version)
        worker_host, _, pid = worker.rpartition(':')
        if worker_host != host or not pid.isdigit():
            continue
        try:
            with open(os.path.join(_PROC, pid, 'cmdline'), 'rb') as cmdline:
                if b'uwsgi' not in cmdline.read():
                    continue
        except OSError:
            continue
        live[worker] = version
    return live
