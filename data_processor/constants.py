# data_processor/constants.py

import os
# dotenv는 파일에 없으므로 제거합니다.

# ----------------------------------------------------------------------
# 1. MongoDB 연결 설정
# ----------------------------------------------------------------------
# 🌟 워커는 이 환경 변수를 통해 마스터/DB의 공인 IP를 받습니다.
MONGO_HOST = os.environ.get('MONGO_HOST', '3.25.153.54')
MONGO_PORT = os.environ.get('MONGO_PORT', '27017')
DB_NAME = os.environ.get('MONGO_DB', 'BBC_analysis_db')
MONGO_USER = os.environ.get('MONGO_USER', 'mongouser')
MONGO_PASS = os.environ.get('MONGO_PASS', '1234')

MONGO_URI = (
    f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/{DB_NAME}"
    "?authSource=admin"
)

# ----------------------------------------------------------------------
# 2. 분산 워커 및 파일 설정
# ----------------------------------------------------------------------
RECORD_NOUNS_COLLECTION = "ImFiles"
FILE_FOLDER_PATH = "data"
TOP_N = 50

# A. 🌟 WORKER_CHUNK_FILES (마스터/워커 모두 가지고 있는 리스트)
WORKER_CHUNK_FILES = {
    "Worker-1": [
        "data/2014.csv",
        "data/2015.csv",
        "data/2016.csv"
    ],
    "Worker-2": [
        "data/2017.csv",
        "data/2018.csv"
    ],
    "Worker-3": [
        "data/2019.csv",
        "data/2020.csv"
    ]
}

# B. 이 인스턴스의 역할 및 할당된 파일 경로 목록
WORKER_NAME = os.environ.get('WORKER_NAME', 'Master')
WORKER_FILE_PATH = WORKER_CHUNK_FILES.get(WORKER_NAME, None) # 마스터에서 요청 시 이 경로를 읽음


# ----------------------------------------------------------------------
# 3. DB 문서 필드 스키마 정의 (MongoDB에 저장될 논리적 필드 이름)
# ----------------------------------------------------------------------
DB_FIELD_HEADING = 'Heading'
DB_FIELD_DATE = 'Date'
DB_FIELD_TAGS = 'Tags'
DB_FIELD_ARTICLES = 'Articles'
DB_FIELD_NOUNS = 'nouns'
DB_FIELD_RECORD_ID = 'RecordID'

# ----------------------------------------------------------------------
# 4. CSV 컬럼명 정의 (CSV 파일의 실제 헤더 이름)
# ----------------------------------------------------------------------
# 🌟 CSV 파일의 실제 헤더에 맞춰 이름을 지정합니다. 🌟
CSV_FIELD_HEADING = 'title'
CSV_FIELD_ARTICLES = 'text'
CSV_FIELD_DATE = 'timestamp'
CSV_FIELD_TAGS = 'tags'
# CSV에 record_id가 없으므로 임시 이름으로 정의하고 importer에서 index를 사용합니다.
CSV_FIELD_RECORD_ID = 'record_id_col_if_exists'

# ----------------------------------------------------------------------
# 5. DB_FIELD_MAPPING 및 CSV_COLUMNS_SOURCE 제거 (importer.py에서 사용하지 않음)
# ----------------------------------------------------------------------
# DB_FIELD_MAPPING은 importer.py에서 제거되므로 constants에서도 제거하거나 주석 처리합니다.
# CSV_COLUMNS_SOURCE는 CSV_FIELD_... 변수가 대체합니다.

DB_FIELD_DEFAULTS = {
    DB_FIELD_TAGS: [],
}

# ----------------------------------------------------------------------
# 6. 고유 명사 추출 제외 목록
# ----------------------------------------------------------------------
EXCLUDE_NOUNS = {
    'mr', 'mrs', 'ms', 'dr', 'prof', 'lord', 'sir', 'madam', 'hon',
    'january', 'february', 'march', 'april', 'may', 'june', 'july', 'august',
    'september', 'october', 'november', 'december',
    'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday',
    'group', 'company', 'year', 'day', 'week', 'month', 'world', 'us', 'uk', 'eu',
    'time', 'service', 'minister', 'government', 'new', 'old', 'get', 'like',
    'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 'ten',
    'jan', 'feb', 'mar', 'apr', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec',
    'i', 'we', 'you', 'he', 'she', 'it', 'they', 'us', 'him', 'her', 'them'
}