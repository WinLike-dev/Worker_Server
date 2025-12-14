# Dockerfile

# Python 3.10-slim 이미지를 기반으로 사용 (가볍고 효율적)
FROM python:3.10-slim

RUN apt-get update && \
    apt-get install -y git netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*

# 작업 디렉토리를 /usr/src/app으로 설정
WORKDIR /usr/src/app
COPY . /usr/src/app/
# requirements.txt 복사 및 의존성 설치
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# [cite_start]TextBlob이 필요로 하는 NLTK 데이터를 다운로드합니다. [cite: 2, 3]
RUN python -m textblob.download_corpora lite

# 프로젝트의 모든 파일(data_processor, worker_server.py 등)을 작업 디렉토리로 복사
COPY . .