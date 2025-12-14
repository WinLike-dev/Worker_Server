# worker_app/views.py

from django.http import JsonResponse
from django.views.decorators.http import require_POST
from data_processor.constants import WORKER_NAME
from data_processor.importer import process_worker_files
import requests
import threading
import os

# 🌟 마스터 서버의 IP는 환경 변수에서 가져올 필요 없이,
# 마스터 서버의 IP를 하드코딩하거나 WORKER_ADDRESSES를 사용해야 하지만,
# 여기서는 간단히 MONGO_HOST를 재사용하는 Flask 방식을 따릅니다.
# (Worker-Master 통신이 실패한다면, 이 MONGO_HOST 설정도 확인해야 합니다.)
MONGO_HOST = os.environ.get('MONGO_HOST', '172.31.30.122')


# 🌟 Phase 2: 마스터에게 완료 상태를 알리는 함수
def notify_master_of_completion(success_status, status_message):
    MASTER_NOTIFICATION_URL = f"http://{MONGO_HOST}:8000/worker_notification" # 8000 포트 가정

    try:
        response = requests.post(
            MASTER_NOTIFICATION_URL,
            json={
                "worker_name": WORKER_NAME,
                "status": "SUCCESS" if success_status else "FAILURE",
                "message": status_message
            },
            timeout=5
        )
        print(f"[{WORKER_NAME}] Phase 2 마스터 알림 시도: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"[{WORKER_NAME}] Phase 2 마스터 알림 실패 (연결 오류): {e}")


# 🌟🌟🌟 Phase 2 실행 및 알림 로직 (백그라운드 스레드에서 실행됨) 🌟🌟🌟
def run_processing_and_notify():
    # ... (생략: Flask의 run_processing_and_notify 함수와 동일한 로직)
    print(f"[{WORKER_NAME}] Phase 2: 데이터 전처리 작업 동기적 실행 시작...")

    success = process_worker_files()

    if success:
        message = f"Data rebuild completed successfully. Worker: {WORKER_NAME}"
    else:
        message = f"Data rebuild failed. Worker: {WORKER_NAME}"

    notify_master_of_completion(success, message)
    print(f"[{WORKER_NAME}] Phase 2 작업 완료 및 마스터에게 최종 알림 전송 완료.")


@require_POST
def handle_rebuild_request(request):
    """
    마스터 요청을 받자마자 즉시 응답(Phase 1)하고, 작업을 백그라운드(Phase 2)로 넘깁니다.
    """
    print(f"[{WORKER_NAME}] Rebuild 요청 수신.")

    # 1. Phase 2 작업을 백그라운드 스레드로 분리하여 시작
    rebuild_thread = threading.Thread(target=run_processing_and_notify)
    rebuild_thread.start()

    # 2. Phase 1: 요청 수신 즉시 마스터에게 응답 반환 (HTTP 202 Accepted)
    return JsonResponse({
        "status": "Accepted",
        "worker_name": WORKER_NAME,
        "message": "Request received successfully. Data rebuild started in background."
    }, status=202)