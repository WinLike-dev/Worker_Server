# data_processor/constants.py

import os

# ----------------------------------------------------------------------
# 1. MongoDB 연결 설정
# ----------------------------------------------------------------------
# 🌟 워커는 이 환경 변수를 통해 마스터/DB의 공인 IP를 받습니다.
MONGO_HOST = os.environ.get('MONGO_HOST', '49.168.187.55')
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
RECORD_NOUNS_COLLECTION = "file_noun_records"
FILE_FOLDER_PATH = "data"
TOP_N = 50

# A. 🌟 워커 이름 및 할당된 파일 경로 목록 🌟 (실제 워커가 처리할 파일)
WORKER_CHUNK_FILES = {
    "Worker-1": [
        "data/2014.csv",
        "data/2015.csv",
        "data/2016.csv"
    ],
    "worker-2": [
        "data/2017.csv",
        "data/2018.csv"
    ],
    "worker-3": [
        "data/2019.csv",
        "data/2020.csv"
    ]
}

# B. 이 인스턴스의 역할 및 할당된 파일 경로 목록
WORKER_NAME = os.environ.get('WORKER_NAME', 'Master')
WORKER_FILE_PATH = WORKER_CHUNK_FILES.get(WORKER_NAME, None)


# ----------------------------------------------------------------------
# 3. DB 문서 필드 스키마 및 CSV 설정
# ----------------------------------------------------------------------
DB_FIELD_HEADING = 'Heading'
DB_FIELD_DATE = 'Date'
DB_FIELD_TAGS = 'Tags'
DB_FIELD_ARTICLES = 'Articles'
DB_FIELD_NOUNS = 'nouns'
DB_FIELD_RECORD_ID = 'record_id'

CSV_COLUMNS_SOURCE = ['title', 'text', 'timestamp', 'tags']

DB_FIELD_MAPPING = {
    'title': DB_FIELD_HEADING,
    'text': DB_FIELD_ARTICLES,
    'timestamp': DB_FIELD_DATE,
    'tags': DB_FIELD_TAGS,
}

DB_FIELD_DEFAULTS = {
    DB_FIELD_TAGS: [],
}

# ----------------------------------------------------------------------
# 4. 고유 명사 추출 제외 목록
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