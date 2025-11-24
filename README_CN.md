# Celery Leased Beat

带有选主功能的 Celery Beat。

此包为 Celery Beat 提供了一个自定义调度器，通过 Redis 实现分布式锁（租约），确保在分布式环境中同一时间只有一个调度器实例处于活动状态。

## 安装

```bash
uv add celery-leased-beat
# 或
pip install celery-leased-beat

# 如果使用 Django
uv add "celery-leased-beat[django]"
# 或
pip install "celery-leased-beat[django]"
```

## 使用

### 配置

在你的 Celery 配置中添加以下设置：

```python
# 必填：用于租约锁的 Redis URL
CELERY_LEASE_URL = 'redis://localhost:6379/0'

# 选填：锁的键名 (默认: 'celery_lease:lock')
CELERY_LEASE_KEY = 'celery_lease:lock'

# 选填：锁的 TTL（秒） (默认: 60)
CELERY_LEASE_LOCK_TTL = 60

# 选填：检查/续租的间隔（秒） (默认: 15)
CELERY_LEASE_INTERVAL = 15

# 选填：额外的 Redis 连接选项
# CELERY_LEASE_OPTIONS = {}
```

#### Redis Sentinel

对于 Redis Sentinel，使用 `sentinel://` 协议并配置多个 sentinel 节点：

```python
CELERY_LEASE_URL = 'sentinel://localhost:26379;sentinel://localhost:26380;sentinel://localhost:26381'
CELERY_LEASE_OPTIONS = {
    'master_name': 'cluster1',
    # 'sentinel_kwargs': {'password': 'password'},  # 如果需要 sentinel_kwargs
}
```

### 运行调度器

你可以使用以下提供的调度器类之一：

*   `celery_leased_beat.scheduler.LeasedScheduler`: 带有选主功能的内存调度器。
*   `celery_leased_beat.scheduler.LeasedPersistentScheduler`: 带有选主功能的持久化调度器 (shelve)。
*   `celery_leased_beat.django.LeasedDjangoScheduler`: 带有选主功能的 Django 数据库调度器 (需要 `django-celery-beat`)。

示例命令：

```bash
celery -A your_project beat -S celery_leased_beat.scheduler.LeasedPersistentScheduler
```

对于 Django：

```bash
celery -A your_project beat -S celery_leased_beat.django.LeasedDjangoScheduler
```

## 开发

本项目使用 `uv` 进行依赖管理，使用 `flit` 进行打包。

```bash
# 安装依赖
uv sync --all-extras

# 运行测试
uv run -- pytest -n auto

# 运行版本矩阵测试
uv tool install tox --with tox-uv
tox
```

## 许可

MIT
