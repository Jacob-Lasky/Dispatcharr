"""Compatibility admission for the persistent plugin, without live services."""
import ast
import importlib.util
import os
from pathlib import Path
import shutil
import sys
import tempfile
from types import ModuleType, SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from apps.m3u import reservation_guard as guard_module


class ReservationGuardCompatibilityTests(TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        source = Path(__file__).resolve().parents[3]
        for name in guard_module.CORE_SHA256:
            destination = self.root / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source / name, destination)
        # The pinned image stamps 0.34.1; its source revision says 0.34.0.
        (self.root / 'version.py').write_text("__version__ = '0.34.1'\n")

    def test_pinned_core_is_accepted(self):
        self.assertEqual(guard_module.compatibility_errors(self.root), [])

    def test_upstream_version_update_is_refused(self):
        (self.root / 'version.py').write_text("__version__ = '0.35.0'\n")
        self.assertIn('expected 0.34.1', guard_module.compatibility_errors(self.root)[0])

    def test_every_hooked_source_change_is_refused_even_at_same_version(self):
        for name in guard_module.CORE_SHA256:
            with self.subTest(name=name):
                path = self.root / name
                original = path.read_bytes()
                path.write_bytes(original + b'\n# changed upstream\n')
                self.assertEqual(guard_module.compatibility_errors(self.root),
                                 [name + ': upstream source differs'])
                path.write_bytes(original)

    def test_missing_or_unreadable_version_and_source_fail_closed(self):
        (self.root / 'version.py').unlink()
        (self.root / 'apps/m3u/connection_pool.py').unlink()
        errors = guard_module.compatibility_errors(self.root)
        self.assertEqual(len(errors), 2)
        self.assertIn('unable to verify upstream version', errors[0])
        self.assertIn('unable to verify upstream source', errors[1])

    def test_invalid_version_assignment_is_refused(self):
        (self.root / 'version.py').write_text('__version__ = calculate_version()\n')
        self.assertEqual(guard_module.compatibility_errors(self.root),
                         ['version.py: unable to verify upstream version'])

    def test_reload_cannot_bypass_check_or_reenable_on_incompatible_core(self):
        existing = SimpleNamespace(reclamation_enabled=False,
            module=SimpleNamespace(GRACE_SECONDS=180, VERSION='0.2.1',
                                   __file__='/data/plugins/m3u_pool_guard/reservation_guard.py'))
        with patch.dict(sys.modules, {guard_module._REGISTRY: existing}), \
             patch.object(guard_module, 'compatibility_errors', return_value=['version mismatch']):
            with self.assertRaisesRegex(RuntimeError, 'refused incompatible core'):
                guard_module.install()
        self.assertFalse(existing.reclamation_enabled)

    def test_supported_reload_preserves_live_guard_and_reports_grace(self):
        existing = SimpleNamespace(reclamation_enabled=False,
            module=SimpleNamespace(GRACE_SECONDS=180, VERSION='0.2.1',
                                   __file__='/data/plugins/m3u_pool_guard/reservation_guard.py'))
        with patch.dict(sys.modules, {guard_module._REGISTRY: existing}), \
             patch.object(guard_module, 'compatibility_errors', return_value=[]), \
             self.assertLogs('apps.m3u.reservation_guard', level='INFO') as logs:
            self.assertIs(guard_module.install(), existing)
        self.assertTrue(existing.reclamation_enabled)
        self.assertIn('grace_seconds=180', logs.output[0])
        self.assertIn('reclamation_enabled=True', logs.output[0])

    def test_adapter_status_reports_live_guard_and_compatibility(self):
        source = Path(__file__).resolve().parents[3] / 'tools/pool_guard/plugin.py'
        package = ModuleType('w466_guard_adapter_test')
        package.__path__ = []
        spec = importlib.util.spec_from_file_location(package.__name__ + '.plugin', source)
        adapter = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(adapter)
        existing = SimpleNamespace(reclamation_enabled=True,
            module=SimpleNamespace(GRACE_SECONDS=180, VERSION='0.2.1',
                                   __file__='/data/plugins/m3u_pool_guard/reservation_guard.py'))
        modules = {package.__name__: package,
                   package.__name__ + '.reservation_guard': guard_module,
                   guard_module._REGISTRY: existing}
        with patch.dict(sys.modules, modules), \
             patch.object(guard_module, 'compatibility_errors', return_value=[]):
            status = adapter.Plugin.__new__(adapter.Plugin).run('status')
        self.assertEqual(status['status'], 'ok')
        self.assertTrue(status['reclamation_enabled'])
        self.assertEqual(status['grace_seconds'], 180)
        self.assertEqual(status['source_file'], existing.module.__file__)
        self.assertEqual(status['guard_version'], '0.2.1')  # the running guard, not the adapter
        with patch.dict(sys.modules, modules), \
             patch.object(guard_module, 'compatibility_errors', return_value=['version mismatch']):
            self.assertEqual(adapter.Plugin.__new__(adapter.Plugin).run('status')['status'], 'error')

    def _adapter(self):
        source = Path(__file__).resolve().parents[3] / 'tools/pool_guard/plugin.py'
        package = ModuleType('w466_guard_adapter_celery_test')
        package.__path__ = []
        spec = importlib.util.spec_from_file_location(package.__name__ + '.plugin', source)
        adapter = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(adapter)
        return package, adapter

    def test_status_from_a_celery_worker_reads_live_web_workers(self):
        """0.34 runs actions on Celery, where the guard is never installed; the
        status must come from the web workers' registration, live ones only."""
        import socket

        package, adapter = self._adapter()
        host = socket.gethostname()
        with tempfile.TemporaryDirectory() as proc:
            for pid, cmd in (('101', b'/dispatcharrpy/bin/uwsgi\0--ini'), ('102', b'celery\0worker')):
                os.makedirs(os.path.join(proc, pid))
                with open(os.path.join(proc, pid, 'cmdline'), 'wb') as f:
                    f.write(cmd)
            registry = {f'{host}:101'.encode(): b'0.2.3', f'{host}:102'.encode(): b'0.2.3',
                        f'{host}:103'.encode(): b'0.2.1', b'otherhost:101': b'0.2.1'}
            client = SimpleNamespace(hgetall=lambda key: registry if key == guard_module.WORKERS else {})
            core_utils = ModuleType('core.utils')
            core_utils.RedisClient = SimpleNamespace(get_client=lambda: client)
            modules = {package.__name__: package,
                       package.__name__ + '.reservation_guard': guard_module,
                       'core.utils': core_utils}
            with patch.dict(sys.modules, modules), patch.object(adapter, '_PROC', proc), \
                 patch.object(guard_module, 'compatibility_errors', return_value=[]):
                sys.modules.pop(guard_module._REGISTRY, None)
                status = adapter.Plugin.__new__(adapter.Plugin).run('status')
                registry.clear()
                empty = adapter.Plugin.__new__(adapter.Plugin).run('status')
        self.assertEqual(status['status'], 'ok')
        self.assertEqual(status['web_workers'], {f'{host}:101': '0.2.3'})
        self.assertEqual(status['guard_version'], '0.2.3')
        self.assertIsNone(status['reclamation_enabled'])
        self.assertEqual(empty['status'], 'error')
        self.assertFalse(empty['installed'])

    def test_adapter_manifest_and_guard_versions_agree(self):
        import json
        tools = Path(__file__).resolve().parents[3] / 'tools/pool_guard'
        manifest = json.loads((tools / 'plugin.json').read_text())['version']
        adapter = next(node.value.value for node in ast.walk(ast.parse((tools / 'plugin.py').read_text()))
                       if isinstance(node, ast.Assign) and getattr(node.targets[0], 'id', None) == 'version')
        self.assertEqual({manifest, adapter, guard_module.VERSION}, {guard_module.VERSION})

    def test_adapter_manifest_is_version_2_and_declares_no_service(self):
        import json
        tools = Path(__file__).resolve().parents[3] / 'tools/pool_guard'
        manifest = json.loads((tools / 'plugin.json').read_text())
        self.assertEqual(manifest['manifest_version'], 2)
        self.assertEqual(manifest['capabilities'], ['proxy_internals'])
        # Per-worker hooks, not a leader service: a manifest-2 plugin with
        # on_leader_acquired but no persistent_service would be skipped, and
        # one with it would guard only the elected process.
        plugin_class = next(node for node in ast.parse((tools / 'plugin.py').read_text()).body
                            if isinstance(node, ast.ClassDef) and node.name == 'Plugin')
        methods = {node.name for node in plugin_class.body if isinstance(node, ast.FunctionDef)}
        self.assertIn('__init__', methods)
        self.assertFalse(methods & {'on_leader_acquired', 'on_leader_lost'})
