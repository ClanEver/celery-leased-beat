import os
from pathlib import Path

import django
import pytest
from celery import Celery
from pytest_docker_tools import container
from pytest_docker_tools.wrappers import Container


@pytest.fixture
def cleanup_schedule_db():
    """Fixture to clean up schedule database files after test."""
    db_files = []

    def register_db_file(filename: str):
        """Register a database file to be cleaned up."""
        db_files.append(filename)

    yield register_db_file

    # Cleanup after test
    for db_file in db_files:
        Path(db_file).unlink(missing_ok=True)
        Path(db_file + '.db').unlink(missing_ok=True)


def pytest_configure():
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'tests.django_settings')
    django.setup()


redis_standalone = container(  # pyright: ignore[reportCallIssue]
    image='redis:8.2.3-alpine3.22',
    ports={
        '6379/tcp': None,
    },
    timeout=60,
    stop_signal='SIGKILL',
    command=[
        'redis-server',
        '--save',
        '',
        '--appendonly',
        'no',
        '--maxmemory-policy',
        'noeviction',
        '--protected-mode',
        'no',
    ],
)
redis_sentinel = container(  # pyright: ignore[reportCallIssue]
    image='clanever/redis-cluster:7.2.5',
    environment={
        'SENTINEL': 'true',
    },
    ports={
        # sentinel
        '5000/tcp': None,
        '5001/tcp': None,
        '5002/tcp': None,
        # master
        '7000/tcp': None,
        '7001/tcp': None,
        '7002/tcp': None,
        # slave
        '7003/tcp': None,
        '7004/tcp': None,
        '7005/tcp': None,
    },
    stop_signal='SIGKILL',
)


@pytest.fixture
def redis_standalone_celery_app(redis_standalone: Container) -> Celery:
    redis_url = f'redis://localhost:{redis_standalone.ports["6379/tcp"][0]}/0'
    app = Celery('test_celery', broker=redis_url)
    app.conf.update(
        CELERY_LEASE_URL=redis_url,
        CELERY_LEASE_KEY='test_cluster_lease',
        CELERY_LEASE_LOCK_TTL=5,
        CELERY_LEASE_INTERVAL=0.1,
    )
    return app
