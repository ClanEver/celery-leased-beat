SECRET_KEY = 'test-secret-key'
DEBUG = True
INSTALLED_APPS = [
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django_celery_beat',
    # 'celery_leased_beat',
]

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': ':memory:',
    },
}

USE_TZ = True
TIME_ZONE = 'UTC'
