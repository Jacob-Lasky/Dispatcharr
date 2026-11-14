from importlib import import_module
from unittest.mock import patch

from django.apps import apps
from django.test import TestCase
from django_celery_beat.models import IntervalSchedule, PeriodicTask

from apps.m3u.models import M3UAccount
from apps.m3u.tasks import _refresh_single_m3u_account_impl, refresh_m3u_accounts, refresh_m3u_groups, refresh_single_m3u_account


class CustomAccountRefreshTests(TestCase):
    def setUp(self):
        self.custom = M3UAccount.get_custom_account()

    def test_save_cannot_schedule_custom_account(self):
        self.custom.refresh_interval = 24
        self.custom.save(update_fields=['refresh_interval'])
        task = PeriodicTask.objects.filter(name=f'm3u_account-refresh-{self.custom.pk}', enabled=True)
        self.assertFalse(task.exists())

    def test_creation_does_not_queue_custom_group_refresh(self):
        self.custom.delete()
        with patch('apps.m3u.signals.refresh_m3u_groups.delay') as delay:
            M3UAccount.objects.create(name='custom', locked=True, refresh_interval=24)
        delay.assert_not_called()

    def test_refresh_all_queues_providers_only(self):
        provider = M3UAccount.objects.create(name='Provider', server_url='https://example.com/list.m3u')
        with patch('apps.m3u.tasks.refresh_single_m3u_account.delay') as delay:
            refresh_m3u_accounts()
        delay.assert_called_once_with(provider.pk)

    def test_direct_refresh_leaves_custom_state_untouched(self):
        with patch('apps.m3u.tasks._set_m3u_account_status') as status:
            result = _refresh_single_m3u_account_impl(self.custom.pk)
        self.assertIn('custom', result.lower())
        status.assert_not_called()

    def test_group_refresh_never_fetches_custom_source(self):
        with patch('apps.m3u.tasks.fetch_m3u_lines') as fetch:
            result, groups = refresh_m3u_groups(self.custom.pk)
        fetch.assert_not_called()
        self.assertIsNone(groups)
        self.assertIn('custom', result.lower())

    def test_provider_named_custom_is_still_refreshable(self):
        self.custom.locked = False
        self.custom.save(update_fields=['locked'])
        self.custom.refresh_interval = 4
        self.custom.save(update_fields=['refresh_interval'])
        self.custom.refresh_from_db()
        self.assertTrue(self.custom.refresh_task.enabled)

    def test_migration_disables_only_custom_schedule_and_resets_error(self):
        provider = M3UAccount.objects.create(name='Provider', refresh_interval=4)
        interval, _ = IntervalSchedule.objects.get_or_create(every=24, period='hours')
        task, _ = PeriodicTask.objects.update_or_create(name=f'm3u_account-refresh-{self.custom.pk}', defaults={
            'task':'apps.m3u.tasks.refresh_single_m3u_account', 'interval':interval,
            'enabled':True, 'kwargs':'{"account_id": 1}'})
        M3UAccount.objects.filter(pk=self.custom.pk).update(refresh_interval=24, refresh_task=task,
            status='error', last_message='No M3U source available (missing URL and file)')
        migration = import_module('apps.m3u.migrations.0020_disable_custom_account_refresh')
        migration.disable_custom_refresh(apps, None)
        migration.disable_custom_refresh(apps, None)
        task.refresh_from_db()
        self.custom.refresh_from_db()
        provider.refresh_from_db()
        self.assertFalse(task.enabled)
        self.assertEqual(self.custom.refresh_interval, 0)
        self.assertEqual(self.custom.status, 'idle')
        self.assertEqual(self.custom.last_message, '')
        self.assertTrue(provider.refresh_task.enabled)

    def test_public_refresh_does_not_mark_interrupted_custom_account_failed(self):
        for state in ('fetching', 'parsing'):
            with self.subTest(state=state):
                M3UAccount.objects.filter(pk=self.custom.pk).update(status=state)
                with patch('apps.m3u.tasks._release_task_db_connection'), patch(
                    'apps.m3u.tasks.log_system_event'
                ) as event:
                    result = refresh_single_m3u_account(self.custom.pk)
                self.custom.refresh_from_db()
                self.assertIn('custom', result.lower())
                self.assertEqual(self.custom.status, state)
                event.assert_not_called()

    def test_migration_recovers_interrupted_custom_refresh(self):
        migration = import_module('apps.m3u.migrations.0020_disable_custom_account_refresh')
        for state in ('fetching', 'parsing'):
            with self.subTest(state=state):
                M3UAccount.objects.filter(pk=self.custom.pk).update(status=state, last_message='Refresh in progress...')
                migration.disable_custom_refresh(apps, None)
                self.custom.refresh_from_db()
                self.assertEqual(self.custom.status, 'idle')
                self.assertEqual(self.custom.last_message, '')
