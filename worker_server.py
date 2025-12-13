# worker_server.py

from flask import Flask, jsonify
from data_processor.constants import WORKER_NAME, MONGO_HOST
from data_processor.importer import process_worker_files
import requests
import threading

app = Flask(__name__)


# 🌟 Phase 2: 마스터에게 완료 상태를 알리는 함수
def notify_master_of_completion(success_status, status_message):
    # 마스터 서버의 IP와 알림 포트를 사용 (Master는 8000 포트를 사용한다고 가정)
    # MONGO_HOST는 환경 변수로 전달받은 마스터/DB의 Public IP가 되어야 합니다.
    MASTER_NOTIFICATION_URL = f"http://{MONGO_HOST}:8000/worker_notification"

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
    """
    백그라운드에서 전처리 작업을 동기적으로 실행하고, 완료 후 마스터에게 알립니다.
    """
    print(f"[{WORKER_NAME}] Phase 2: 데이터 전처리 작업 동기적 실행 시작...")

    # 1. 데이터 전처리 작업 동기적 실행 (Blocking the background thread)
    success = process_worker_files()

    # 2. 알림 메시지 구성
    if success:
        message = f"Data rebuild completed successfully. Worker: {WORKER_NAME}"
    else:
        message = f"Data rebuild failed. Worker: {WORKER_NAME}"

    # 3. 마스터에게 최종 완료 상태를 알림
    notify_master_of_completion(success, message)
    print(f"[{WORKER_NAME}] Phase 2 작업 완료 및 마스터에게 최종 알림 전송 완료.")


@app.route('/status', methods=['GET'])
def get_status():
    return jsonify({
        "status": "Ready",
        "worker_name": WORKER_NAME,
        "message": "Data processing completed. Waiting for master commands."
    })


# 🌟🌟🌟 /rebuild 엔드포인트: Phase 1 (즉시 응답) 🌟🌟🌟
@app.route('/rebuild', methods=['POST'])
def handle_rebuild_request():
    """
    마스터 요청을 받자마자 즉시 응답(Phase 1)하고, 작업을 백그라운드(Phase 2)로 넘깁니다.
    """
    print(f"[{WORKER_NAME}] Rebuild 요청 수신.")

    # 1. Phase 2 작업을 백그라운드 스레드로 분리하여 시작
    rebuild_thread = threading.Thread(target=run_processing_and_notify)
    rebuild_thread.start()

    # 2. Phase 1: 요청 수신 즉시 마스터에게 응답 반환 (HTTP 202 Accepted)
    return jsonify({
        "status": "Accepted",
        "worker_name": WORKER_NAME,
        "message": "Request received successfully. Data rebuild started in background."
    }), 202


if __name__ == '__main__':
    # WORKER_NAME에 따라 포트를 설정
    port = 8001
    if WORKER_NAME == "Worker-2":
        port = 8002
    elif WORKER_NAME == "Worker-3":
        port = 8003

    app.run(host='0.0.0.0', port=port)