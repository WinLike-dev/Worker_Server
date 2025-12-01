# Dockerfile

# Python 3.10-slim 이미지를 기반으로 사용 (가볍고 효율적)
FROM python:3.10-slim

# 작업 디렉토리를 /usr/src/app으로 설정
WORKDIR /usr/src/app

# requirements.txt.txt 복사 및 의존성 설치
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# TextBlob이 필요로 하는 NLTK 데이터를 다운로드합니다.
# 이 과정이 없으면 TextBlob 사용 시 오류가 발생할 수 있습니다.
RUN python -m textblob.download_corpora lite

# 프로젝트의 모든 파일(data_processor, worker_server.py 등)을 작업 디렉토리로 복사
COPY . .

# 🌟 워커 노드 실행 명령어: worker_server.py 실행으로 변경 🌟
CMD ["python", "worker_server.py"]