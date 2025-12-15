# data_processor/importer.py

from typing import List
import pandas as pd
from textblob import TextBlob
import os
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
    할당된 CSV 파일을 읽어 명사를 추출하고 MongoDB에 저장합니다.
    """
    client = None
    total_success = True

    try:
        # 1. DB 연결
        client = get_mongodb_client()
        if client is None:
            return False

        db = client[DB_NAME]
        collection = db[RECORD_NOUNS_COLLECTION]

        if not WORKER_FILE_PATH:
            print(f"[{WORKER_NAME}] ⚠️ 처리할 파일이 없습니다.")
            return True

        print(f"[{WORKER_NAME}] 총 {len(WORKER_FILE_PATH)}개의 파일을 처리합니다.")

        # 2. 파일 순회
        for file_path in WORKER_FILE_PATH:
            try:
                print(f"[{WORKER_NAME}] ➡️ 파일 로드 중: {file_path}")

                # CSV 파일 읽기 (인코딩 에러 방지)
                try:
                    df = pd.read_csv(file_path, encoding='utf-8')
                except UnicodeDecodeError:
                    df = pd.read_csv(file_path, encoding='cp949')

                documents_to_insert = []
                print(f"[{WORKER_NAME}]    - 데이터 처리 시작 ({len(df)}행)...")

                # 3. 행(Row) 단위 처리 (여기가 핵심입니다!)
                for index, row in df.iterrows():
                    try:
                        # 컬럼명 확인 필수! (csv 파일의 헤더와 일치해야 함)
                        title = str(row.get('title', ''))
                        content = str(row.get('content', ''))
                        link = str(row.get('link', ''))

                        # 제목과 내용을 합쳐서 분석
                        full_text = f"{title} {content}"

                        # 🌟🌟🌟 핵심 함수 호출 🌟🌟🌟
                        nouns = extract_and_filter_proper_nouns(full_text)

                        # 추출된 명사가 있을 경우에만 문서 생성
                        if nouns:
                            doc = {
                                "title": title,
                                "link": link,
                                "nouns": nouns,  # 추출된 명사 리스트
                                "worker_name": WORKER_NAME,
                                "source_file": os.path.basename(file_path)
                            }
                            documents_to_insert.append(doc)

                    except Exception as row_e:
                        # 한 행이 에러나도 멈추지 않고 계속 진행
                        continue

                # 4. DB 일괄 삽입 (Batch Insert)
                if documents_to_insert:
                    collection.insert_many(documents_to_insert)
                    print(f"[{WORKER_NAME}]    - ✨ {len(documents_to_insert)}건 DB 저장 완료.")
                else:
                    print(f"[{WORKER_NAME}]    - ⚠️ 저장할 데이터가 없습니다 (명사 추출 실패).")

                print(f"[{WORKER_NAME}] ✅ 파일 처리 완료: {file_path}")

            except FileNotFoundError:
                print(f"[{WORKER_NAME}] ❌ 파일을 찾을 수 없음: {file_path}")
                total_success = False
            except Exception as e:
                print(f"[{WORKER_NAME}] ❌ 파일 처리 중 오류 ({file_path}): {e}")
                total_success = False

    except Exception as e:
        print(f"[{WORKER_NAME}] ❌ 치명적 오류 발생: {e}")
        total_success = False

    finally:
        # 5. DB 연결 해제
        close_mongodb_client(client)

    return total_success