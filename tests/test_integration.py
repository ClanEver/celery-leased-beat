import tempfile

from celery import Celery

from celery_leased_beat.scheduler import LeasedPersistentScheduler, LeasedScheduler


def test_leased_scheduler_integration(redis_standalone_celery_app: Celery):
    """
    Integration test for LeasedScheduler using a real Redis container.
    Only uses Redis as broker and backend, no RabbitMQ or Celery workers needed.
    """
    app = redis_standalone_celery_app
    scheduler = LeasedScheduler(app=app, schedule_filename='schedule_test_leased')

    # Initial state: not acquired
    assert not scheduler._lease_lock_acquired

    # First tick should acquire the lock
    scheduler.tick()
    assert scheduler._lease_lock_acquired

    # Verify lock exists in Redis
    client = scheduler.lease_redis_client
    assert client.get(scheduler.lease_lock_key) is not None

    # Cleanup
    scheduler.close()
    assert client.get(scheduler.lease_lock_key) is None


def test_leased_persistent_scheduler_integration(redis_standalone_celery_app: Celery, cleanup_schedule_db):
    """
    Integration test for LeasedPersistentScheduler using a real Redis container.
    Only uses Redis as broker and backend, no RabbitMQ or Celery workers needed.
    """
    app = redis_standalone_celery_app

    temp_file = tempfile.mkstemp()[1]
    cleanup_schedule_db(temp_file)
    scheduler = LeasedPersistentScheduler(app=app, schedule_filename=temp_file)

    # Initial state: not acquired
    assert not scheduler._lease_lock_acquired

    # First tick should acquire the lock
    scheduler.tick()
    assert scheduler._lease_lock_acquired

    # Verify lock exists in Redis
    client = scheduler.lease_redis_client
    assert client.get(scheduler.lease_lock_key) is not None

    # Cleanup
    scheduler.close()
    assert client.get(scheduler.lease_lock_key) is None
