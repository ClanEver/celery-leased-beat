from celery_leased_beat.scheduler import LeasedSchedulerMixin

try:
    from django_celery_beat.schedulers import DatabaseScheduler
except ImportError as e:
    raise ImportError('Need to install django-celery-beat to use LeasedDjangoScheduler') from e


class LeasedDjangoScheduler(LeasedSchedulerMixin, DatabaseScheduler):
    pass


__all__ = ['LeasedDjangoScheduler']
