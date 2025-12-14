# worker_project/urls.py

from django.urls import path, include

urlpatterns = [
    # 워커 앱의 URL을 포함합니다.
    path('', include('worker_app.urls')),
]