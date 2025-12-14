# data_processor/importer.py

from typing import List
import pandas as pd
from textblob import TextBlob
from .db_connector import get_mongodb_client, close_mongodb_client
from .constants import (
    WORKER_NAME, WORKER_FILE_PATH,
    DB_NAME, RECORD_NOUNS_COLLECTION, EXCLUDE_NOUNS,
    # DB_FIELD_MAPPING 제거
    DB_FIELD_DEFAULTS,
    # 🌟 DB 필드명 임포트
    DB_FIELD_HEADING, DB_FIELD_DATE, DB_FIELD_TAGS, DB_FIELD_ARTICLES,
    DB_FIELD_NOUNS, DB_FIELD_RECORD_ID,
    # 🌟 CSV 필드명 임포트 (추가)
    CSV_FIELD_HEADING, CSV_FIELD_DATE, CSV_FIELD_TAGS, CSV_FIELD_ARTICLES,
    CSV_FIELD_RECORD_ID
)
import warnings
import sys

warnings.filterwarnings('ignore')


# ----------------------------------------------------------------------
# 유틸리티 함수 (기존 코드 유지)
# ----------------------------------------------------------------------

def extract_and_filter_proper_nouns(text) -> List[str]:
    """TextBlob을 사용하여 고유 명사를 추출하고, 제외 목록에 있는 단어를 필터링합니다."""
    if pd.isna(text) or text is None:
        return []

    text = str(text).replace('\n', ' ')

    try:
        blob = TextBlob(text)
        # NNP/NNPS 태그된 단어 중 제외 목록을 거르고, 길이 1 또는 숫자인 단어를 제거
        filtered_nouns = [
            word.lower()
            for word, tag in blob.tags
            if tag in ('NNP', 'NNPS') and
               word.lower() not in EXCLUDE_NOUNS and
               len(word) > 1 and not word.isdigit()
        ]

        return filtered_nouns
    except Exception as e:
        print(f"ERROR: TextBlob 처리 중 오류 발생: {e}")
        return []


def parse_tags(tags_str: str) -> List[str]:
    """문자열 형태의 태그 목록을 파싱하여 소문자 리스트로 반환합니다."""
    if not tags_str:
        return DB_FIELD_DEFAULTS.get(DB_FIELD_TAGS, [])

    tags_str = tags_str.strip().strip('[]').replace("'", "")
    if not tags_str:
        return DB_FIELD_DEFAULTS.get(DB_FIELD_TAGS, [])

    return [tag.strip().lower() for tag in tags_str.split(',') if tag.strip()]


# ----------------------------------------------------------------------
# MongoDB 연결 및 데이터 처리 함수
# ----------------------------------------------------------------------

def process_worker_files() -> bool:
    """
    워커에게 할당된 CSV 파일 목록(WORKER_FILE_PATH)만 처리하고, DB 연결을 명시적으로 종료합니다.
    """
    client = None  # MongoDB 클라이언트 변수 초기화
    total_success = False

    try:
        # 1. MongoDB 연결 획득 (get_mongodb_client는 새로운 인스턴스를 반환)
        client = get_mongodb_client()
        if client is None:
            return False  # 연결 실패 시 False 반환

        # 2. 할당 파일 목록 검사 (이전에 누락되었던 로직)
        if not WORKER_FILE_PATH:
            print(f"⚠️ 경고: 워커 '{WORKER_NAME}'에게 할당된 파일 목록(WORKER_FILE_PATH)이 없습니다. 작업을 건너뜁니다.")
            return True

        print(f"[{WORKER_NAME}] 총 {len(WORKER_FILE_PATH)}개의 할당된 CSV 파일을 처리합니다.")

        db = client[DB_NAME]
        record_collection = db[RECORD_NOUNS_COLLECTION]

        current_success = True

        # 3. 파일 처리 루프 (CSV 파일 처리 로직)
        for file_path in WORKER_FILE_PATH:
            try:
                print(f"[{WORKER_NAME}] ➡️ 파일 처리 시작: {file_path}")
                df = pd.read_csv(file_path)

                # 🌟 [여기에 기존 CSV 처리 및 DB 삽입 로직이 실행됩니다] 🌟
                # ... (예: df를 순회하며 명사 추출 및 DB 삽입)

                print(f"[{WORKER_NAME}] ✅ 파일 처리 완료: {file_path}")

            except FileNotFoundError:
                print(f"[{WORKER_NAME}] ❌ 파일 누락 오류: CSV 파일 '{file_path}'이 컨테이너에 없습니다.", file=sys.stderr)
                current_success = False
            except Exception as e:
                print(f"[{WORKER_NAME}] ❌ 파일 처리 중 알 수 없는 오류 발생 ({file_path}): {e}", file=sys.stderr)
                current_success = False

        total_success = current_success  # 루프 종료 후 최종 성공 여부 결정

    except Exception as e:
        print(f"[{WORKER_NAME}] ❌ 최상위 처리 오류 발생: {e}", file=sys.stderr)
        total_success = False

    finally:
        # 4. 🌟 가장 중요: 함수 종료 전 반드시 연결 해제 🌟
        close_mongodb_client(client)

    return total_success