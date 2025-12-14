# data_processor/db_connector.py (수정)

from pymongo import MongoClient
from .constants import MONGO_URI, WORKER_NAME
import sys

# 🌟 전역 변수 _mongo_client 제거 (데드락 방지) 🌟
# _mongo_client = None

def get_mongodb_client():
    """MongoDB 클라이언트 인스턴스를 반환합니다. (매번 새 연결 시도)"""
    try:
        # 매번 새로운 연결을 시도하여 스레드 간 충돌 방지
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        print(f"[{WORKER_NAME}] MongoDB 연결 성공.")
        return client
    except Exception as e:
        print(f"[{WORKER_NAME}] ❌ MongoDB 연결 오류 발생: {e}", file=sys.stderr)
        return None


def close_mongodb_client(client):
    """특정 MongoDB 연결을 종료합니다. (인스턴스를 인자로 받음)"""
    # 🌟 인스턴스를 인자로 받아 처리 🌟
    if client:
        client.close()
        print(f"[{WORKER_NAME}] MongoDB 연결 해제.")