from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# 스크립트 및 로그 경로 설정
scripts_dir = '/opt/airflow/scripts'
logs_dir = '/opt/airflow/logs'

# 각 스크립트의 import
from scripts.download_osm_data import download_osm_data
from scripts.convert_osm_to_geojson import convert_osm_to_geojson
from scripts.geojson_mongodb import geojson_mongodb
from scripts.create_map import create_map

# 기본 DAG 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
with DAG(
    'osm_to_geojson_to_html_mongodb',
    default_args=default_args,
    description='OpenStreetMap 데이터를 가져와서 GeoJSON으로 변환 후 MongoDB에 저장하고 HTML로 시각화합니다.',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    # Task 1: OSM 데이터 다운로드
    download_task = PythonOperator(
        task_id='download_osm_data',
        python_callable=download_osm_data,
        op_kwargs={
            'relation_id': '12351758',  # 임의의 Relation ID
            'output_path': f'{logs_dir}/railway_network.osm',
        },
    )

    # Task 2: OSM 데이터를 GeoJSON으로 변환
    convert_task = PythonOperator(
        task_id='convert_osm_to_geojson',
        python_callable=convert_osm_to_geojson,
        op_kwargs={
            'osm_file': f'{logs_dir}/railway_network.osm',
            'output_file': f'{logs_dir}/railway_network.geojson',
        },
    )

    # Task 3: GeoJSON 데이터를 MongoDB에 저장
    geojson_mongodb_task = PythonOperator(
        task_id='geojson_mongodb',
        python_callable=geojson_mongodb,
        op_kwargs={
            'geojson_file': f'{logs_dir}/railway_network.geojson',
            'mongo_uri': 'mongodb+srv://ryumdoh:6FbMhsklN352kvKp@cluster0.ytjgxbj.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0',  # MongoDB URI
            'db_name': 'mydatabase',
            'collection_name': 'osm_geojson1',
        },
    )

    # Task 4: HTML 파일 생성
    create_map_task = PythonOperator(
        task_id='create_html_map',
        python_callable=create_map,
        op_kwargs={
            'geojson_file': f'{logs_dir}/railway_network.geojson',
            'html_file': f'{logs_dir}/osmgeojson_html_map.html',  # HTML 파일을 logs 폴더에 저장
        },
    )

    # Task 순서 정의
    download_task >> convert_task >> geojson_mongodb_task >> create_map_task
