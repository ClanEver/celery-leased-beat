<div dir="ltr" align=center>

**ENGLISH 🇺🇸** / [**简体中文 🇨🇳**](README_cn.md)

</div>

# Celery Leased Beat

Celery Beat with leader election using Redis.

This package provides a custom scheduler for Celery Beat that ensures only one instance of the scheduler is active at a time in a distributed environment. It uses Redis to acquire a distributed lock (lease).

## Installation

```bash
uv add celery-leased-beat
# or
pip install celery-leased-beat

# For Django support
uv add "celery-leased-beat[django]"
# or
pip install "celery-leased-beat[django]"
```

## Usage

### Configuration

Add the following configuration to your Celery settings:

```python
# Required: Redis URL for the lease lock
CELERY_LEASE_URL = 'redis://localhost:6379/0'

# Optional: Key name for the lock (default: 'celery_lease:lock')
CELERY_LEASE_KEY = 'celery_lease:lock'

# Optional: Lock TTL in seconds (default: 60)
CELERY_LEASE_LOCK_TTL = 60

# Optional: Interval to check/renew the lease in seconds (default: 15)
CELERY_LEASE_INTERVAL = 15

# Optional: Additional Redis connection options
# CELERY_LEASE_OPTIONS = {}
```

#### Redis Sentinel

For Redis Sentinel, use the `sentinel://` scheme with multiple sentinel nodes:

```python
CELERY_LEASE_URL = 'sentinel://localhost:26379;sentinel://localhost:26380;sentinel://localhost:26381'
CELERY_LEASE_OPTIONS = {
    'master_name': 'cluster1',
    # 'sentinel_kwargs': {'password': 'password'},  # If sentinel_kwargs is needed
}
```

### Running the Scheduler

You can use one of the provided scheduler classes:

*   `celery_leased_beat.scheduler.LeasedScheduler`: In-memory scheduler with leader election.
*   `celery_leased_beat.scheduler.LeasedPersistentScheduler`: Persistent scheduler (shelve) with leader election.
*   `celery_leased_beat.django.LeasedDjangoScheduler`: Django database scheduler with leader election (requires `django-celery-beat`).

Example command:

```bash
celery -A your_project beat -S celery_leased_beat.scheduler.LeasedPersistentScheduler
```

For Django:

```bash
celery -A your_project beat -S celery_leased_beat.django.LeasedDjangoScheduler
```

## Development

This project uses `uv` for dependency management and `flit` for packaging.

```bash
# Install dependencies
uv sync --all-extras

# Run tests
uv run -- pytest -n auto

# Run matrix tests
uv tool install tox --with tox-uv
tox
```

## License

MIT
