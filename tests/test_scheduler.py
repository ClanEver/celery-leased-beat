import time
from unittest.mock import MagicMock

import pytest
from celery import Celery
from redis.exceptions import LockError

from celery_leased_beat.scheduler import LeasedScheduler


@pytest.fixture
def app():
    app = Celery('test_app')
    app.conf.update(
        CELERY_LEASE_URL='redis://localhost:6379/0',
        CELERY_LEASE_KEY='test_lock',
        CELERY_LEASE_LOCK_TTL=10,
        CELERY_LEASE_INTERVAL=1,
    )
    return app


@pytest.fixture
def mock_redis(mocker):
    mock_redis_cls = mocker.patch('celery_leased_beat.lease_mixin.Redis')
    mock_redis_instance = MagicMock()
    mock_redis_cls.from_url.return_value = mock_redis_instance
    return mock_redis_instance


@pytest.fixture
def mock_scheduler_deps(mocker):
    mocker.patch('celery.beat.shelve')
    mock_tick = mocker.patch('celery.beat.Scheduler.tick', return_value=0)
    mocker.patch('celery.beat.Scheduler.close')
    return mock_tick


def test_acquire_lock_success(app, mock_redis, mock_scheduler_deps):
    mock_tick = mock_scheduler_deps

    scheduler = LeasedScheduler(app=app, schedule_filename='schedule')

    # Mock lock object
    mock_lock = MagicMock()
    mock_redis.lock.return_value = mock_lock
    mock_lock.acquire.return_value = True

    # First tick should try to acquire
    scheduler.tick()

    # Check lock creation
    mock_redis.lock.assert_called_with('test_lock', timeout=10, thread_local=False)
    # Check acquire
    mock_lock.acquire.assert_called_with(blocking=False, token=scheduler._lease_lock_id)

    assert scheduler._lease_lock_acquired
    mock_tick.assert_called()


def test_acquire_lock_failure(app, mock_redis, mock_scheduler_deps):
    mock_tick = mock_scheduler_deps

    scheduler = LeasedScheduler(app=app, schedule_filename='schedule')

    mock_lock = MagicMock()
    mock_redis.lock.return_value = mock_lock
    mock_lock.acquire.return_value = False

    # First tick should try to acquire and fail
    result = scheduler.tick()

    mock_lock.acquire.assert_called_with(blocking=False, token=scheduler._lease_lock_id)
    assert not scheduler._lease_lock_acquired
    mock_tick.assert_not_called()
    assert result == 1


def test_renew_lock_success(app, mock_redis, mock_scheduler_deps):
    mock_tick = mock_scheduler_deps

    scheduler = LeasedScheduler(app=app, schedule_filename='schedule')
    scheduler._lease_lock_acquired = True

    mock_lock = MagicMock()
    mock_redis.lock.return_value = mock_lock
    # Inject the mock lock into the scheduler instance since we are simulating already acquired state
    scheduler._lease_lock = mock_lock

    scheduler.tick()

    mock_lock.reacquire.assert_called_with()
    assert scheduler._lease_lock_acquired
    mock_tick.assert_called()


def test_renew_lock_tolerance_success(app, mock_redis, mock_scheduler_deps):
    mock_tick = mock_scheduler_deps

    scheduler = LeasedScheduler(app=app, schedule_filename='schedule')
    scheduler._lease_lock_acquired = True

    mock_lock = MagicMock()
    mock_redis.lock.return_value = mock_lock
    scheduler._lease_lock = mock_lock

    # Simulate one failure (LockError)
    mock_lock.reacquire.side_effect = LockError('Temporary failure')

    # First tick with failure
    scheduler.tick()

    # Should still hold lock because threshold (60/15 - 1 = 3) > 1
    assert scheduler._lease_lock_acquired
    mock_tick.assert_not_called()

    # Reset side effect for next call to simulate recovery
    mock_lock.reacquire.side_effect = None

    scheduler.tick()
    assert scheduler._lease_lock_acquired
    assert scheduler._lease_renew_fail_count == 0
    mock_tick.assert_called()


def test_renew_lock_tolerance_failure(app, mock_redis):
    scheduler = LeasedScheduler(app=app, schedule_filename='schedule')
    scheduler._lease_lock_acquired = True
    # Set values to get threshold = 3 (60 // 15 - 1 = 3)
    scheduler.lease_lock_ttl = 60
    scheduler.lease_interval = 15
    scheduler._lease_renew_threshold = max(1, scheduler.lease_lock_ttl // scheduler.lease_interval - 1)

    mock_lock = MagicMock()
    mock_redis.lock.return_value = mock_lock
    scheduler._lease_lock = mock_lock

    mock_lock.reacquire.side_effect = LockError('Persistent failure')

    # Threshold is 3.
    # 1st fail
    scheduler.tick()
    assert scheduler._lease_lock_acquired

    # 2nd fail
    scheduler.tick()
    assert scheduler._lease_lock_acquired

    # 3rd fail should step down
    scheduler.tick()
    assert not scheduler._lease_lock_acquired


def test_renew_lock_failure(app, mock_redis):
    scheduler = LeasedScheduler(app=app, schedule_filename='schedule')
    scheduler._lease_lock_acquired = True
    # Set low threshold to fail quickly. TTL=10, Interval=10 => Threshold = max(1, 0) = 1.
    # Count=1 < 1 False. So fails on 1st try.
    scheduler.lease_lock_ttl = 10
    scheduler.lease_interval = 10
    scheduler._lease_renew_threshold = max(1, scheduler.lease_lock_ttl // scheduler.lease_interval - 1)

    mock_lock = MagicMock()
    mock_redis.lock.return_value = mock_lock
    scheduler._lease_lock = mock_lock

    # reacquire raises LockError if failed (e.g. lost lock)
    mock_lock.reacquire.side_effect = LockError('Lost lock')

    scheduler.tick()

    assert not scheduler._lease_lock_acquired
