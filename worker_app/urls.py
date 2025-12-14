# worker_app/urls.py

from django.urls import path
from . import views

urlpatterns = [
    # 마스터 서버의 master_connector.py에서 호출하는 엔드포인트와 일치해야 합니다.
    path('rebuild', views.handle_rebuild_request, name='rebuild'),
]