# data_processor/importer.py

from typing import List
import pandas as pd
from textblob import TextBlob
from .db_connector import get_mongodb_client
from .constants import (
    WORKER_NAME, WORKER_FILE_PATH,
    DB_NAME, RECORD_NOUNS_COLLECTION, EXCLUDE_NOUNS,
    DB_FIELD_MAPPING, DB_FIELD_DEFAULTS,
    DB_FIELD_HEADING, DB_FIELD_DATE, DB_FIELD_TAGS, DB_FIELD_ARTICLES,
    DB_FIELD_NOUNS, DB_FIELD_RECORD_ID
)
import warnings
import sys

warnings.filterwarnings('ignore')


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


def process_worker_files() -> bool:
    """
    워커에게 할당된 CSV 파일 목록을 읽어 각 레코드의 명사를 추출하고 MongoDB에 저장합니다.
    작업 성공 여부(bool)를 반환합니다.
    """
    if WORKER_NAME == 'Master' or not WORKER_FILE_PATH:
        print(f"[{WORKER_NAME}] 워커 작업 실행: 파일을 처리하지 않고 건너뜁니다.")
        return True  # 처리할 파일이 없으면 성공으로 간주

    print(f"[{WORKER_NAME}] 워커 작업 시작. 할당 파일 목록: {WORKER_FILE_PATH}")

    client = get_mongodb_client()  # db_connector.py에서 독립적인 연결 생성
    if client is None:
        return False  # DB 연결 실패 시 바로 종료

    success = False  # 작업 성공 여부 플래그

    try:
        # --- 1. 데이터 로드 및 전처리 ---
        all_dataframes = []
        for file_path in WORKER_FILE_PATH:
            print(f"🔄 파일 로드 중: {file_path}")
            df_chunk = pd.read_csv(file_path, encoding='utf-8')
            all_dataframes.append(df_chunk)

        df = pd.concat(all_dataframes, ignore_index=True)
        print(f"✅ 총 {len(all_dataframes)}개 파일 로드 완료. 전체 레코드: {len(df)}")

        # CSV 컬럼과 DB 필드 이름 매핑
        df = df.rename(columns={
            csv_col: db_col
            for csv_col, db_col in DB_FIELD_MAPPING.items()
            if csv_col in df.columns
        })

        # 필수 컬럼 검사
        required_db_cols = list(DB_FIELD_MAPPING.values())
        if not all(col in df.columns for col in required_db_cols):
            missing = [col for col in required_db_cols if col not in df.columns]
            raise ValueError(f"필수 컬럼 누락: {missing}. CSV 컬럼과 DB_FIELD_MAPPING을 확인하세요.")

        # 데이터 정리 및 타입 변환
        df[DB_FIELD_DATE] = pd.to_datetime(df[DB_FIELD_DATE], errors='coerce').dt.strftime('%Y-%m-%d')
        df[DB_FIELD_ARTICLES] = df[DB_FIELD_ARTICLES].fillna('')
        df[DB_FIELD_HEADING] = df[DB_FIELD_HEADING].fillna('')
        df[DB_FIELD_TAGS] = df[DB_FIELD_TAGS].fillna('')

        # MongoDB에 저장할 때 사용할 고유 식별자(index)를 추가
        df[DB_FIELD_RECORD_ID] = df.index

        # --- 2. 명사 추출 및 DB 삽입 ---
        db = client[DB_NAME]
        record_collection = db[RECORD_NOUNS_COLLECTION]

        # **주의:** 워커가 데이터를 추가/재생성할 때 기존 데이터를 지우는 로직이 필요한지 확인 후 사용
        # record_collection.delete_many({})

        documents_to_insert = []
        total_records = len(df)

        print("--- 레코드별 명사 추출 및 MongoDB 직접 저장 시작 (file_noun_records) ---")

        for index, row in df.iterrows():
            combined_text = str(row[DB_FIELD_HEADING]) + ' ' + str(row[DB_FIELD_ARTICLES])
            nouns = extract_and_filter_proper_nouns(combined_text)
            parsed_tags = parse_tags(str(row[DB_FIELD_TAGS]))

            document = {
                DB_FIELD_RECORD_ID: int(row[DB_FIELD_RECORD_ID]),
                DB_FIELD_HEADING: str(row[DB_FIELD_HEADING]),
                DB_FIELD_DATE: str(row[DB_FIELD_DATE]),
                DB_FIELD_TAGS: parsed_tags,
                DB_FIELD_NOUNS: nouns,
                "noun_count": len(nouns)
            }
            documents_to_insert.append(document)

            if (index + 1) % 1000 == 0:
                print(f"처리 진행 중: {index + 1}/{total_records} 레코드")

        if documents_to_insert:
            record_collection.insert_many(documents_to_insert)
            print(f"✅ 총 {len(documents_to_insert)}개 레코드를 '{RECORD_NOUNS_COLLECTION}'에 성공적으로 저장했습니다.")
        else:
            print("⚠️ 경고: 저장할 레코드가 없습니다.")

        success = True  # 모든 작업이 오류 없이 완료됨

    except Exception as e:
        print(f"ERROR: 워커 데이터 처리 및 저장 중 오류 발생: {e}", file=sys.stderr)
        success = False

    finally:
        # 🌟 중요: 작업 성공/실패와 관계없이 독립 연결을 닫아줍니다. 🌟
        if client:
            client.close()
            print(f"[{WORKER_NAME}] Importer 작업 완료 후 독립 연결 해제.")

    return success