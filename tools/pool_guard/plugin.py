"""No-restart adapter for the core reservation guard on Dispatcharr 0.33."""
import os
import sys


class Plugin:
    name = 'M3U Reservation Guard'
    version = '0.1.0'
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
        guard = sys.modules.get('_dispatcharr_m3u_pool_guard')
        return {'status': 'ok' if guard else 'error', 'installed': bool(guard),
                'worker_pid': os.getpid(), 'version': self.version}

    def stop(self, context=None):
        # No threads are created. Keep ownership-aware release until in-flight
        # leases drain; disabling only stops reclamation in this worker.
        guard = sys.modules.get('_dispatcharr_m3u_pool_guard')
        if guard:
            guard.reclamation_enabled = False
