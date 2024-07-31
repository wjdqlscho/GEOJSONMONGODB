import os
import geojson
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ConfigurationError, ServerSelectionTimeoutError
import certifi  # SSL 인증서 문제를 해결하기 위한 패키지

# MongoDB 연결 설정
def connect_to_mongo(uri='mongodb+srv://ryumdoh:6FbMhsklN352kvKp@cluster0.ytjgxbj.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0',
                     db_name='mydatabase',
                     collection_name='osm_geojson1'):
    try:
        # MongoDB 클라이언트 생성, SSL 인증서 검증을 위한 certifi 사용
        client = MongoClient(uri, serverSelectionTimeoutMS=5000, tlsCAFile=certifi.where())
        db = client[db_name]
        collection = db[collection_name]
        # 서버에 연결 가능한지 테스트
        client.admin.command('ping')
        print("MongoDB에 성공적으로 연결되었습니다.")
        return collection
    except (ConnectionFailure, ConfigurationError, ServerSelectionTimeoutError) as e:
        print(f"MongoDB 연결 실패: {e}")
        return None

# 중복 데이터 제거
def remove_duplicates(features):
    seen = set()
    unique_features = []
    for feature in features:
        feature_id = feature.get('properties', {}).get('id', str(feature))  # 'id'가 없을 경우 feature 자체를 사용
        if feature_id not in seen:
            seen.add(feature_id)
            unique_features.append(feature)
    return unique_features

# GeoJSON 파일을 읽고 MongoDB에 저장
def process_geojson_to_mongo(geojson_file, mongo_collection):
    # GeoJSON 파일 로드
    with open(geojson_file, 'r', encoding='utf-8') as f:
        data = geojson.load(f)

    # 중복 제거
    features = data.get('features', [])
    unique_features = remove_duplicates(features)

    # MongoDB에 저장
    if mongo_collection is not None:
        try:
            mongo_collection.delete_many({})  # 기존 데이터 삭제 (선택적)
            if unique_features:
                mongo_collection.insert_many(unique_features)  # 한 번에 많은 데이터를 삽입
                print(f"{len(unique_features)}개의 데이터가 MongoDB에 저장되었습니다.")
            else:
                print("저장할 데이터가 없습니다.")
        except Exception as e:
            print(f"데이터 저장 실패: {e}")
    else:
        print("MongoDB 연결 실패로 데이터를 저장하지 못했습니다.")

if __name__ == "__main__":
    # 컨테이너 내부 경로 설정
    geojson_file = "/opt/airflow/logs/railway_network.geojson"

    # MongoDB 연결 및 데이터 처리
    mongo_collection = connect_to_mongo()
    process_geojson_to_mongo(geojson_file, mongo_collection)
