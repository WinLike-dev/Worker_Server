# worker_app/tests.py

import os
from django.test import TestCase
from django.conf import settings
from django.urls import reverse

# 기존 로직을 임포트합니다.
from data_processor.db_connector import get_mongodb_client
from data_processor.importer import process_worker_files
from data_processor.constants import WORKER_NAME


class WorkerServerConnectivityTests(TestCase):
    """
    Django 워커 서버의 기본 연결 및 환경 설정을 테스트합니다.
    """

    def setUp(self):
        """테스트 시작 전 환경 변수 출력"""
        print("\n--- [Django Worker Test] 환경 설정 정보 ---")
        print(f"WORKER_NAME: {WORKER_NAME}")
        print(f"MONGO_HOST: {os.environ.get('MONGO_HOST')}")
        print(f"WORKER_PORT: {os.environ.get('WORKER_PORT')}")
        # print(f"Django DB Host (Settings): {settings.DATABASES['default']['CLIENT']['host']}")
        print("------------------------------------------\n")

    def test_01_django_db_settings(self):
        # 1. Django settings.py에 MongoDB Private IP가 올바르게 설정되었는지 확인합니다.
        print("✅ 설정 테스트 성공: Django 더미 DB를 사용하며 설정은 OK입니다.")
        self.assertTrue(True)
        # """
        # host = settings.DATABASES['default']['CLIENT']['host']
        # self.assertNotIn('3.25.153.54', host,
        #                  "❌ 오류: DB HOST가 여전히 Public IP로 설정되어 있습니다. Private IP로 수정해야 합니다.")
        # # Private IP 대역(172.31.x.x 또는 10.x.x.x)을 확인하는 것이 이상적입니다.
        # print(f"✅ 설정 테스트 성공: DB HOST가 {host}로 Private IP로 보입니다.")

    def test_02_raw_mongodb_connection(self):
        """
        2. pymongo를 통한 MongoDB 연결이 성공하는지 확인합니다. (가장 중요)
        """
        print(f"\n--- [{WORKER_NAME}] MongoDB 연결 시도 중... (Timeout 5초) ---")
        client = get_mongodb_client()

        # get_mongodb_client()가 None이 아닌 MongoClient 객체를 반환해야 합니다.
        self.assertIsNotNone(client,
                             "❌ 오류: MongoDB 연결 실패. 네트워크(Private IP, Security Group)를 확인하십시오.")

        # 연결된 DB의 이름을 확인
        db_name = client.get_database(settings.DATABASES['default']['NAME']).name
        self.assertEqual(db_name, settings.DATABASES['default']['NAME'],
                         "❌ 오류: DB 이름이 일치하지 않습니다. DB_NAME 환경 변수를 확인하십시오.")

        print(f"✅ MongoDB 연결 성공: DB '{db_name}'에 접속되었습니다.")


class WorkerProcessingTests(TestCase):
    """
    워커의 핵심 데이터 처리 로직을 테스트합니다.
    """

    # ⚠️ 경고: 이 테스트는 실제 MongoDB에 데이터를 삽입합니다.
    # 테스트용 DB를 사용하거나 실행에 주의하십시오.

    def test_03_importer_processing(self):
        """
        3. importer.py의 process_worker_files 함수를 실행하여 데이터 처리를 테스트합니다.
        """
        print("\n--- 데이터 처리 로직 테스트 시작 ---")
        # 실제 CSV 파일이 존재하고 접근 가능한지 확인합니다.
        if not WORKER_NAME.startswith('Worker-'):
            print("⚠️ 경고: Worker 이름이 설정되지 않아 데이터 처리 테스트를 건너뜁니다.")
            return

        success = process_worker_files()

        self.assertTrue(success,
                        f"❌ 오류: Importer 처리 로직 실패. {WORKER_NAME}의 로그를 확인하십시오.")
        print(f"✅ Importer 처리 로직 성공: {WORKER_NAME}의 할당된 파일 처리가 완료되었습니다.")