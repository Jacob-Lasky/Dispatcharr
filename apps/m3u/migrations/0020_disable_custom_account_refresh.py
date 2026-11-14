from django.db import migrations
from django.db.models import Q
from django.utils import timezone


def disable_custom_refresh(apps, schema_editor):
    Account = apps.get_model('m3u', 'M3UAccount')
    Task = apps.get_model('django_celery_beat', 'PeriodicTask')
    Changed = apps.get_model('django_celery_beat', 'PeriodicTasks')
    alias = schema_editor.connection.alias if schema_editor else 'default'
    for account in Account.objects.using(alias).filter(name='custom', locked=True):
        Task.objects.using(alias).filter(
            name=f'm3u_account-refresh-{account.pk}',
            task='apps.m3u.tasks.refresh_single_m3u_account',
        ).update(enabled=False)
        # Queryset update bypasses account signals while historical models migrate.
        Account.objects.using(alias).filter(pk=account.pk).update(refresh_interval=0)
        Account.objects.using(alias).filter(pk=account.pk).filter(
            Q(status__in=['fetching', 'parsing']) |
            Q(status='error', last_message='No M3U source available (missing URL and file)'),
        ).update(status='idle', last_message='')
    # Queryset updates bypass Beat's save hook; notify its database scheduler.
    Changed.objects.using(alias).update_or_create(ident=1, defaults={'last_update': timezone.now()})


class Migration(migrations.Migration):
    dependencies = [
        ('m3u', '0019_m3uaccountprofile_exp_date'),
        ('django_celery_beat', '0019_alter_periodictasks_options'),
    ]
    operations = [migrations.RunPython(disable_custom_refresh, migrations.RunPython.noop)]
