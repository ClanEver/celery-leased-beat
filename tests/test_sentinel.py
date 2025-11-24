import time

import pytest
from celery import Celery
from pytest_docker_tools.wrappers import Container

from celery_leased_beat.scheduler import LeasedScheduler


@pytest.fixture
def celery_app(redis_sentinel: Container) -> Celery:
    redis_sentinel.reload()
    app = Celery('test_sentinel')
    redis_url = f'sentinel://localhost:{redis_sentinel.ports["5000/tcp"][0]}'
    app.conf.update(
        CELERY_LEASE_URL=redis_url,
        CELERY_LEASE_LOCK_TTL=1,
        CELERY_LEASE_INTERVAL=0.25,
        CELERY_LEASE_OPTIONS={
            'master_name': 'sentinel7000',
        },
    )
    return app


def test_sentinel_lease_acquire(celery_app):
    # Wait for sentinel to sync
    time.sleep(5)

    scheduler = LeasedScheduler(app=celery_app)

    # Try to acquire lock
    scheduler.tick()

    assert scheduler._lease_lock_acquired is True
    assert scheduler.lease_redis_client.ping() is True

    scheduler.close()
