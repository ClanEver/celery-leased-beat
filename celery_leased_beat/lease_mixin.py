import socket
import os
import time
import uuid
import logging
import urllib.parse
from celery.beat import Scheduler
from redis import Redis
from redis.sentinel import Sentinel
from redis.cluster import RedisCluster
from redis.exceptions import RedisError, LockError
from redis.lock import Lock
from typing import cast, TYPE_CHECKING

if TYPE_CHECKING:
    __MixinBase = Scheduler
else:
    __MixinBase = object

logger = logging.getLogger(__name__)


class LeasedSchedulerMixin(__MixinBase):
    """
    Scheduler that uses Redis to acquire a lock before processing tasks.
    Only the instance holding the lock will schedule tasks.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.lease_url: str = self.app.conf.get('CELERY_LEASE_URL')
        if not self.lease_url:
            raise ValueError('CELERY_LEASE_URL must be set')
        self.lease_lock_key: str = self.app.conf.get('CELERY_LEASE_KEY', 'celery_lease:lock')
        self.lease_lock_ttl: int = self.app.conf.get('CELERY_LEASE_LOCK_TTL', 60)
        self.lease_interval: int = self.app.conf.get('CELERY_LEASE_INTERVAL', 15)
        self.lease_options: dict = self.app.conf.get('CELERY_LEASE_OPTIONS', {})

        hostname = socket.gethostname()
        pid = os.getpid()
        self._lease_lock_id: str = f'{hostname}-{pid}-{uuid.uuid4()}'

        self._lease_redis_client: Redis | RedisCluster | None = None
        self._lease_lock: Lock | None = None
        self._lease_lock_acquired: bool = False
        self._lease_renew_threshold: int = max(1, self.lease_lock_ttl // self.lease_interval - 1)
        self._lease_renew_fail_count: int = 0
        self._lease_last_acquire_time: float = 0.0

    @property
    def lease_redis_client(self) -> Redis | RedisCluster:
        if self._lease_redis_client is None:
            if self.lease_url.startswith('sentinel://'):
                if not self.lease_options.get('master_name'):
                    raise ValueError('CELERY_LEASE_OPTIONS.master_name must be set for sentinel')

                sentinels = []
                for url in self.lease_url.split(';'):
                    if not url.strip():
                        continue
                    parsed = urllib.parse.urlparse(url)
                    sentinels.append((parsed.hostname, parsed.port))

                conn_params = {
                    k: v for k, v in self.lease_options.items() if k not in ('master_name', 'sentinel_kwargs')
                }
                sentinel = Sentinel(
                    sentinels,
                    sentinel_kwargs=self.lease_options.get('sentinel_kwargs', {}),
                    **conn_params,
                )
                self._lease_redis_client = sentinel.master_for(
                    self.lease_options['master_name'],
                    **conn_params,
                )
            else:
                self._lease_redis_client = Redis.from_url(self.lease_url, **self.lease_options)
        return self._lease_redis_client

    @property
    def lease_lock(self) -> Lock:
        if self._lease_lock is None:
            # Create a lock object with our custom ID
            self._lease_lock = self.lease_redis_client.lock(
                self.lease_lock_key,
                timeout=self.lease_lock_ttl,
                thread_local=False,
            )
        return cast(Lock, self._lease_lock)

    @property
    def _time_since_last_lease(self) -> float:
        return time.monotonic() - self._lease_last_acquire_time

    def _acquire_lock(self):
        try:
            # blocking=False means it returns True/False immediately
            acquired = self.lease_lock.acquire(blocking=False, token=self._lease_lock_id)
            if acquired:
                self._lease_last_acquire_time = time.monotonic()
                if not self._lease_lock_acquired:
                    logger.info(
                        "Acquired lock '%s' with id %s. Becoming leader.",
                        self.lease_lock_key,
                        self._lease_lock_id,
                    )
                self._lease_lock_acquired = True
                self._lease_renew_fail_count = 0
                return True
            return False
        except (RedisError, LockError) as e:
            logger.error('Redis error during acquire: %s', e)
            return False

    def _renew_lock(self):
        try:
            _ = self.lease_lock.reacquire()
            self._lease_last_acquire_time = time.monotonic()
            self._lease_renew_fail_count = 0
            logger.debug('Renewed lock.')
            return True
        except (RedisError, LockError) as e:
            self._lease_renew_fail_count += 1
            if self._lease_renew_fail_count < self._lease_renew_threshold:
                logger.warning(
                    'Failed to renew lock (count %d/%d), skip temporary failure: %s',
                    self._lease_renew_fail_count,
                    self._lease_renew_threshold,
                    e,
                )
                return False

            logger.warning('Failed to renew lock (lost ownership): %s', e)
            self._lease_lock_acquired = False
            return False

    def tick(self, *args, **kwargs) -> int | float:
        """
        Run a tick of the scheduler.
        """
        if self._lease_lock_acquired:
            if self._time_since_last_lease < self.lease_interval:
                pass
            elif not self._renew_lock():
                return self.lease_interval
        elif self._acquire_lock():
            # Just acquired.
            pass
        else:
            # Failed to acquire.
            logger.debug('Failed to acquire lock, sleeping for %s seconds.', self.lease_interval)
            return self.lease_interval

        # If we are here, we hold the lock
        return min(
            self.lease_interval - self._time_since_last_lease,
            super().tick(*args, **kwargs),
        )

    def close(self):
        """Release the lock on close."""
        if self._lease_lock_acquired:
            try:
                self.lease_lock.local.token = self._lease_lock_id
                self.lease_lock.release()
                logger.info('Released lock.')
            except (RedisError, LockError) as e:
                logger.error('Error releasing lock: %s', e)
        super().close()


__all__ = ['LeasedSchedulerMixin']
