import time
from datetime import timedelta
from unittest.mock import MagicMock

import pytest
from celery import Celery
from django.utils import timezone
from django_celery_beat.models import ClockedSchedule, IntervalSchedule, PeriodicTask

from celery_leased_beat.django import LeasedDjangoScheduler


@pytest.mark.django_db(transaction=True)
def test_leased_django_scheduler_integration(redis_standalone_celery_app: Celery):
    """
    Integration test for LeasedDjangoScheduler using a real Redis broker provided by pytest-celery
    and a SQLite database provided by pytest-django.
    """
    app = redis_standalone_celery_app

    # Create a periodic task in the DB
    schedule, _ = IntervalSchedule.objects.get_or_create(
        every=10,
        period=IntervalSchedule.SECONDS,
    )
    PeriodicTask.objects.create(
        interval=schedule,
        name='test_task',
        task='celery_leased_beat.tests.tasks.test_task',
    )

    scheduler = LeasedDjangoScheduler(app=app)

    # Initial state: not acquired
    assert not scheduler._lease_lock_acquired

    # First tick should acquire the lock
    scheduler.tick()
    assert scheduler._lease_lock_acquired

    # Verify lock exists in Redis
    client = scheduler.lease_redis_client
    assert client.get(scheduler.lease_lock_key) is not None

    # Verify scheduler loaded tasks from DB
    assert 'test_task' in scheduler.schedule

    # Cleanup
    scheduler.close()
    assert client.get(scheduler.lease_lock_key) is None


@pytest.mark.django_db(transaction=True)
def test_leased_django_scheduler_failover(redis_standalone_celery_app: Celery):
    """
    Failover test for LeasedDjangoScheduler.
    Scenario:
    - 2 Schedulers, LOCK_TTL=0.75, INTERVAL=0.25
    - Task 1: Interval 0.25s
    - Task 2: Clocked T+1.25s
    - 0-0.75s: Scheduler 1 runs, Scheduler 2 fails to acquire
    - 0.75s: Scheduler 1 closes
    - 0.75-1.75s: Scheduler 2 acquires lock and runs Task 2
    """
    app = redis_standalone_celery_app

    # Configuration
    app.conf.update(
        CELERY_LEASE_LOCK_TTL=0.75,
        CELERY_LEASE_INTERVAL=0.25,
    )

    # Create Tasks
    # 1. Interval Task (0.25s)
    interval_schedule, _ = IntervalSchedule.objects.get_or_create(every=250000, period=IntervalSchedule.MICROSECONDS)
    PeriodicTask.objects.create(
        interval=interval_schedule,
        name='interval_task',
        task='celery_leased_beat.tests.tasks.interval_task',
    )

    # 2. Clocked Task (T+1.25s)
    run_time = timezone.now() + timedelta(seconds=1.25)
    clocked_schedule = ClockedSchedule.objects.create(clocked_time=run_time)
    PeriodicTask.objects.create(
        clocked=clocked_schedule,
        name='clocked_task',
        task='celery_leased_beat.tests.tasks.clocked_task',
        one_off=True,
    )

    # Instantiate Schedulers
    # We mock the apply_async method to track task execution without actually running them in a worker
    # (since we are testing the scheduler logic here)
    mock_apply_async = MagicMock()

    class TestScheduler(LeasedDjangoScheduler):
        def send_task(self, *args, **kwargs):
            mock_apply_async(*args, **kwargs)
            return super().send_task(*args, **kwargs)

    scheduler1 = TestScheduler(app=app)
    scheduler2 = TestScheduler(app=app)

    # Phase 1: 0-0.75s
    print('\n--- Phase 1: Scheduler 1 should lead ---')
    for i in range(3):
        print(f'Iteration {i} (Time {i * 0.25}s)')
        scheduler1.tick()
        scheduler2.tick()

        assert scheduler1._lease_lock_acquired, f'Scheduler 1 should have lock at iter {i}'
        assert not scheduler2._lease_lock_acquired, f'Scheduler 2 should NOT have lock at iter {i}'
        time.sleep(0.25)

    # Verify Scheduler 1 sent interval tasks
    # It should have sent at least a few times
    assert mock_apply_async.call_count >= 1

    # Phase 2: Scheduler 1 closes
    print('\n--- Phase 2: Scheduler 1 closes ---')
    scheduler1.close()

    # Reset mock to track what Scheduler 2 does
    mock_apply_async.reset_mock()

    # Phase 3: 0.75-1.75s (Scheduler 2 takes over)
    print('\n--- Phase 3: Scheduler 2 should take over ---')
    # Give it a moment to realize lock is free or wait for interval
    time.sleep(0.3)

    for i in range(4):
        print(f'Iteration {i} (Time {0.75 + 0.3 + i * 0.25}s)')
        scheduler2.tick()

        if i > 0:  # Might take one tick to acquire
            assert scheduler2._lease_lock_acquired, f'Scheduler 2 should have lock at iter {i}'

        time.sleep(0.25)

    # Verify Scheduler 2 sent the clocked task (and maybe interval tasks)
    # Check if 'clocked_task' was sent
    calls = [call.args[0] for call in mock_apply_async.call_args_list]
    print(f'Tasks sent by Scheduler 2: {calls}')
    assert 'celery_leased_beat.tests.tasks.clocked_task' in calls

    scheduler2.close()


@pytest.mark.django_db(transaction=True)
def test_leased_django_scheduler_crash(redis_standalone_celery_app: Celery):  # noqa: PLR0915
    """
    Crash Failover test for LeasedDjangoScheduler.
    Scenario:
    - 2 Schedulers, LOCK_TTL=0.75, INTERVAL=0.25
    - Task 1: Interval 0.25s
    - Task 2: Clocked T+1.75s
    - 0-0.75s: Scheduler 1 runs.
    - 0.75s: Scheduler 1 "crashes" (stops ticking, NO close).
    - 0.75-2.5s: Scheduler 2 runs.
        - Initially fails to acquire (lock held by dead S1).
        - After TTL (0.75s), lock expires.
        - S2 acquires lock and runs Task 2.
    """
    app = redis_standalone_celery_app

    # Configuration
    app.conf.update(
        CELERY_LEASE_LOCK_TTL=0.75,
        CELERY_LEASE_INTERVAL=0.25,
    )

    # Create Tasks
    # 1. Interval Task (0.25s)
    interval_schedule, _ = IntervalSchedule.objects.get_or_create(every=250000, period=IntervalSchedule.MICROSECONDS)
    PeriodicTask.objects.create(
        interval=interval_schedule,
        name='crash_interval_task',
        task='celery_leased_beat.tests.tasks.crash_interval_task',
    )

    # 2. Clocked Task (T+1.75s)
    run_time = timezone.now() + timedelta(seconds=1.75)
    clocked_schedule = ClockedSchedule.objects.create(clocked_time=run_time)
    PeriodicTask.objects.create(
        clocked=clocked_schedule,
        name='crash_clocked_task',
        task='celery_leased_beat.tests.tasks.crash_clocked_task',
        one_off=True,
    )

    # Instantiate Schedulers
    mock_apply_async = MagicMock()
    def mock_calls():
        return [call.args[0] for call in mock_apply_async.call_args_list]

    class TestScheduler(LeasedDjangoScheduler):
        def send_task(self, *args, **kwargs):
            print(f'Sending task {args[0]}')
            mock_apply_async(*args, **kwargs)
            return super().send_task(*args, **kwargs)

    scheduler1 = TestScheduler(app=app)
    scheduler2 = TestScheduler(app=app)

    # Phase 1: 0-0.75s (Scheduler 1 runs)
    print('\n--- Phase 1: Scheduler 1 runs ---')
    for i in range(3):
        print(f'Iteration {i} (Time {i * 0.25}s)')
        tick_sleep = scheduler1.tick()
        assert tick_sleep <= 0.25
        while tick_sleep <= 0:
            tick_sleep = scheduler1.tick()
            assert tick_sleep <= 0.25
        assert scheduler1._lease_lock_acquired
        scheduler2.tick()
        assert not scheduler2._lease_lock_acquired
        time.sleep(0.25)

    assert mock_calls() == ['celery_leased_beat.tests.tasks.crash_interval_task'] * 2

    # Phase 2: Scheduler 1 crashes (we just stop ticking it)
    print('\n--- Phase 2: Scheduler 1 crashes (stops ticking) ---')
    # Do NOT call scheduler1.close()

    # Phase 3: 0.75-1.75s (Scheduler 2 tries to take over)
    print('\n--- Phase 3: Scheduler 2 tries to take over ---')

    # S1 stopped at T=0.5, lock expires at T=0.5+0.75=1.25.
    # sleep 0.25
    # S2 starts at T=0.75.
    # Iter 0: 0.75s.
    # Iter 1: 1.00s.
    # Iter 2: 1.25s. (Should acquire)
    # Iter 3: 1.50s.
    # Iter 4: 1.75s.
    for i in range(5):
        mock_apply_async.reset_mock()
        print(f'Iteration {i} (Time {0.75 + i * 0.25}s)')
        tick_sleep = scheduler2.tick()
        assert tick_sleep <= 0.25
        while tick_sleep <= 0:
            tick_sleep = scheduler2.tick()
            assert tick_sleep <= 0.25

        if i <= 1:
            assert not scheduler2._lease_lock_acquired
            assert mock_calls() == []
        elif i == 2:
            assert scheduler2._lease_lock_acquired
            print('!!! Scheduler 2 successfully acquired lock at iter 2 !!!')
            assert mock_calls() == ['celery_leased_beat.tests.tasks.crash_interval_task']
        elif i == 4:
            assert scheduler2._lease_lock_acquired
            assert 'celery_leased_beat.tests.tasks.crash_interval_task' in mock_calls()
            assert 'celery_leased_beat.tests.tasks.crash_clocked_task' in mock_calls()
        else:
            assert scheduler2._lease_lock_acquired
            assert mock_calls() == ['celery_leased_beat.tests.tasks.crash_interval_task']

        time.sleep(0.25)

    # Clean up
    scheduler1.close()
    scheduler2.close()
