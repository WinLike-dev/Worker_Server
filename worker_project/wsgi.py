# /worker_project/wsgi.py 파일에 저장

import os

from django.core.wsgi import get_wsgi_application

# 이 설정은 Django에게 worker_project 패키지 내의 settings.py를 사용하라고 지시합니다.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'worker_project.settings')

application = get_wsgi_application()