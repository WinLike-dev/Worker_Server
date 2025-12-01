# data_processor/db_connector.py

from pymongo import MongoClient
from .constants import MONGO_URI, WORKER_NAME
import sys

# 전역 클라이언트 변수 (연결 재사용을 위해)
_mongo_client = None


def get_mongodb_client():
    """MongoDB 클라이언트 인스턴스를 반환합니다."""
    global _mongo_client

    if _mongo_client is not None:
        return _mongo_client

    try:
        # 클라이언트 연결 시도
        _mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        # 연결 확인 (실제 쿼리를 보내지 않고 연결 상태만 확인)
        _mongo_client.admin.command('ping')
        print(f"[{WORKER_NAME}] MongoDB 연결 성공.")
        return _mongo_client
    except Exception as e:
        print(f"[{WORKER_NAME}] ❌ MongoDB 연결 오류 발생: {e}", file=sys.stderr)
        _mongo_client = None
        return None


def close_mongodb_client():
    """MongoDB 연결을 종료합니다."""
    global _mongo_client
    if _mongo_client:
        _mongo_client.close()
        _mongo_client = None
        print(f"[{WORKER_NAME}] MongoDB 연결 해제.")

# __init__.py 파일은 비어있어도 무방합니다.