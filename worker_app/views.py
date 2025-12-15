# worker_app/views.py (수정)

from django.http import JsonResponse
from django.views.decorators.http import require_POST
from django.views.decorators.csrf import csrf_exempt
from data_processor.constants import WORKER_NAME
from data_processor.importer import process_worker_files  # <-- 이 함수만 사용
import time  # 시간 측정을 위해 추가
import sys


# 🌟🌟🌟 run_processing_and_notify 함수와 notify_master_of_completion 함수는 주석 처리하거나 제거합니다. 🌟🌟🌟
# 🌟 (더 이상 백그라운드 프로세스와 별도 알림 로직이 필요 없음) 🌟

@csrf_exempt
@require_POST
def handle_rebuild_request(request):
    """
    Master의 요청을 받고, 작업을 동기적으로 실행 후, 완료 시점에 최종 응답을 반환합니다.
    """
    print(f"[{WORKER_NAME}] 📩 Rebuild 요청 수신.")
    start_time = time.time()  # 요청 처리 시작 시간 측정

    success = False
    message = ""

    try:
        # 🌟 1. 핵심 데이터 처리 함수를 현재 스레드에서 실행 (Blocking) 🌟
        print(f"[{WORKER_NAME}] ⚙️ 데이터 전처리 작업 동기적 실행 시작...")
        success = process_worker_files()

        end_time = time.time()
        processing_time = end_time - start_time

        if success:
            message = f"Data rebuild completed successfully. Worker: {WORKER_NAME}"
            # 2. 작업 완료 후 200 OK 응답 반환
            return JsonResponse({
                "status": "COMPLETED",
                "worker_name": WORKER_NAME,
                "message": message,
                "processing_time": processing_time,  # 총 처리 시간을 포함하여 마스터에게 전달
            }, status=200)
        else:
            message = f"Data rebuild failed. Check worker logs. Worker: {WORKER_NAME}"
            return JsonResponse({
                "status": "FAILED",
                "worker_name": WORKER_NAME,
                "message": message,
                "processing_time": processing_time,
            }, status=500)  # 작업 실패 시 500 에러 반환

    except Exception as e:
        end_time = time.time()
        processing_time = end_time - start_time
        print(f"[{WORKER_NAME}] ❌ 치명적 오류 발생: {e}", file=sys.stderr)
        return JsonResponse({
            "status": "CRITICAL_ERROR",
            "message": str(e),
            "processing_time": processing_time,
        }, status=500)