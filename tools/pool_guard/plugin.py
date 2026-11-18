"""No-restart adapter for the core reservation guard on Dispatcharr 0.34.1."""
import os
import sys


class Plugin:
    name = 'M3U Reservation Guard'
    version = '0.2.0'
    description = 'Tracks provider slot ownership and reclaims abandoned reservations.'
    actions = [{'id': 'status', 'label': 'Status'}]

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
        return {'status': 'ok' if guard and not errors else 'error', 'installed': bool(guard),
                'compatible': not errors, 'compatibility_errors': errors,
                'core_version': reservation_guard.SUPPORTED_VERSION,
                'core_revision': reservation_guard.UPSTREAM_REVISION,
                'reclamation_enabled': bool(guard and guard.reclamation_enabled),
                'grace_seconds': guard.module.GRACE_SECONDS if guard else None,
                'source_file': guard.module.__file__ if guard else None,
                'worker_pid': os.getpid(), 'version': self.version}

    def stop(self, context=None):
        # No threads are created. Keep ownership-aware release until in-flight
        # leases drain; disabling only stops reclamation in this worker.
        guard = sys.modules.get('_dispatcharr_m3u_pool_guard')
        if guard:
            guard.reclamation_enabled = False
