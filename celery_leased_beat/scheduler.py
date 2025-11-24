from celery.beat import Scheduler, PersistentScheduler
from celery_leased_beat.lease_mixin import LeasedSchedulerMixin


class LeasedScheduler(LeasedSchedulerMixin, Scheduler):
    pass


class LeasedPersistentScheduler(LeasedSchedulerMixin, PersistentScheduler):
    pass


__all__ = [
    'LeasedPersistentScheduler',
    'LeasedScheduler',
]
