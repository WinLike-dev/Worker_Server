# data_processor/importer.py

from typing import List
import pandas as pd
from textblob import TextBlob
from .db_connector import get_mongodb_client
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
    워커에게 할당된 CSV 파일 목록을 읽어 각 레코드의 명사를 추출하고 MongoDB에 저장합니다.
    작업 성공 여부(bool)를 반환합니다.
    """
    if WORKER_NAME == 'Master' or not WORKER_FILE_PATH:
        print(f"[{WORKER_NAME}] 워커 작업 실행: 파일을 처리하지 않고 건너뜁니다.")
        return True

    print(f"[{WORKER_NAME}] 워커 작업 시작. 할당 파일 목록: {WORKER_FILE_PATH}")

    client = get_mongodb_client()
    if client is None:
        return False

    success = False

    try:
        # --- 1. 데이터 로드 및 전처리 ---
        all_dataframes = []
        for file_path in WORKER_FILE_PATH:
            print(f"🔄 파일 로드 중: {file_path}")
            df_chunk = pd.read_csv(file_path, encoding='utf-8')
            all_dataframes.append(df_chunk)

        df = pd.concat(all_dataframes, ignore_index=True)
        print(f"✅ 총 {len(all_dataframes)}개 파일 로드 완료. 전체 레코드: {len(df)}")

        # 🌟 df.rename(columns=DB_FIELD_MAPPING) 로직 제거 🌟
        # CSV_FIELD_... 변수를 사용하여 원본 컬럼에 접근합니다.

        # 필수 컬럼 검사
        required_csv_cols = [CSV_FIELD_HEADING, CSV_FIELD_ARTICLES, CSV_FIELD_DATE, CSV_FIELD_TAGS]
        if not all(col in df.columns for col in required_csv_cols):
            missing = [col for col in required_csv_cols if col not in df.columns]
            raise ValueError(f"필수 컬럼 누락: {missing}. CSV 파일 헤더를 확인하세요.")

        # 데이터 정리 및 타입 변환 (CSV_FIELD_... 사용)
        df[CSV_FIELD_DATE] = pd.to_datetime(df[CSV_FIELD_DATE], errors='coerce').dt.strftime('%Y-%m-%d')
        df[CSV_FIELD_ARTICLES] = df[CSV_FIELD_ARTICLES].fillna('')
        df[CSV_FIELD_HEADING] = df[CSV_FIELD_HEADING].fillna('')
        df[CSV_FIELD_TAGS] = df[CSV_FIELD_TAGS].fillna('')

        # --- 2. 명사 추출 및 DB 삽입 ---
        db = client[DB_NAME]
        record_collection = db[RECORD_NOUNS_COLLECTION]

        documents_to_insert = []
        total_records = len(df)

        print("--- 레코드별 명사 추출 및 MongoDB 직접 저장 시작 (file_noun_records) ---")

        for index, row in df.iterrows():
            try:
                # 텍스트 접근: CSV_FIELD_... 변수 사용
                combined_text = str(row[CSV_FIELD_HEADING]) + ' ' + str(row[CSV_FIELD_ARTICLES])

                nouns = extract_and_filter_proper_nouns(combined_text)
                parsed_tags = parse_tags(str(row[CSV_FIELD_TAGS]))

                # RecordID 처리: CSV 컬럼이 DataFrame에 없으면, row.get()은 index를 반환하여 KeyError 방지
                record_id_value = int(row.get(CSV_FIELD_RECORD_ID, index))

                document = {
                    # DB 필드명(Key)에 CSV 필드 값(Value)을 할당합니다.
                    DB_FIELD_RECORD_ID: record_id_value,

                    DB_FIELD_HEADING: str(row[CSV_FIELD_HEADING]),
                    DB_FIELD_DATE: str(row[CSV_FIELD_DATE]),
                    DB_FIELD_TAGS: parsed_tags,
                    DB_FIELD_ARTICLES: str(row[CSV_FIELD_ARTICLES]),
                    DB_FIELD_NOUNS: nouns,
                    "noun_count": len(nouns)
                }
                documents_to_insert.append(document)

            except KeyError as e:
                # 필수 CSV 컬럼이 누락된 경우 (e.g. 'title'이나 'text'가 없는 경우)
                print(f"ERROR: 데이터 처리 중 필수 CSV 컬럼 누락 오류: {e}. 해당 레코드(Index {index})를 건너뜁니다.", file=sys.stderr)
                continue
            except Exception as e:
                print(f"ERROR: 데이터 처리 중 알 수 없는 오류 발생 (Index {index}): {e}", file=sys.stderr)
                continue

            if (index + 1) % 1000 == 0:
                print(f"처리 진행 중: {index + 1}/{total_records} 레코드")

        if documents_to_insert:
            record_collection.insert_many(documents_to_insert)
            print(f"✅ 총 {len(documents_to_insert)}개 레코드를 '{RECORD_NOUNS_COLLECTION}'에 성공적으로 저장했습니다.")
        else:
            print("⚠️ 경고: 저장할 레코드가 없습니다.")

        success = True

    except Exception as e:
        print(f"ERROR: 워커 데이터 처리 및 저장 중 오류 발생: {e}", file=sys.stderr)
        success = False

    finally:
        if client:
            client.close()
            print(f"[{WORKER_NAME}] Importer 작업 완료 후 독립 연결 해제.")

    return success