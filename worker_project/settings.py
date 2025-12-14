# worker_project/settings.py

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = 'django-insecure-dummy-key-for-worker'

DEBUG = True # ⚠️ 운영 환경에서는 False로 설정해야 합니다.

ALLOWED_HOSTS = ['*'] # 모든 호스트 허용 (Docker 환경에서는 필수)

INSTALLED_APPS = [
    # 'django.contrib.admin', # 💡 워커는 필요 없음
    # 'django.contrib.auth',
    # 'django.contrib.contenttypes',
    # 'django.contrib.sessions',
    # 'django.contrib.messages',
    # 'django.contrib.staticfiles',
    'data_processor', # 기존 로직 폴더를 앱으로 사용
    'worker_app', # 워커 뷰를 위한 앱
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    # 'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    # 'django.middleware.csrf.CsrfViewMiddleware', # CSRF는 REST API 워커에서 필요 없음
    # 'django.contrib.auth.middleware.AuthenticationMiddleware',
    # 'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'worker_project.urls'
WSGI_APPLICATION = 'worker_project.wsgi.application'


# 🌟 MongoDB 설정 (환경 변수를 통해 Private IP 주입) 🌟
MONGO_HOST = os.environ.get('MONGO_HOST', '172.31.30.122')
MONGO_PORT = os.environ.get('MONGO_PORT', '27017')
DB_NAME = os.environ.get('MONGO_DB', 'BBC_analysis_db')
MONGO_USER = os.environ.get('MONGO_USER', 'mongouser')
MONGO_PASS = os.environ.get('MONGO_PASS', '1234')

DATABASES = {
    'default': {
        # ❌ Djongo 제거 후, Django의 기본 DB로 변경
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': BASE_DIR / 'db.sqlite3',
    }
}

# ⚠️ Timezone 및 기타 설정은 마스터와 동일하게 설정합니다.
TIME_ZONE = 'UTC'
USE_TZ = True