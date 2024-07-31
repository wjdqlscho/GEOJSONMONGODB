import requests
import os

def download_osm_data(relation_id, output_path):
    try:
        # 디렉토리가 존재하지 않으면 생성
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        print(f"Directory created or exists: {os.path.dirname(output_path)}")
    except PermissionError as e:
        print(f"PermissionError: {e}")
        return
    except Exception as e:
        print(f"Unexpected error: {e}")
        return

    # OSM 데이터 다운로드
    url = f"https://www.openstreetmap.org/api/0.6/relation/{relation_id}/full"
    response = requests.get(url)

    if response.status_code == 200:
        print(f"OSM data download successful: {url}")
        # 데이터 저장
        with open(output_path, "wb") as file:
            file.write(response.content)
            print(f"OSM data saved to: {output_path}")
    else:
        print(f"OSM data download failed: Status code {response.status_code}")

if __name__ == "__main__":
    # 컨테이너 내부 경로 설정
    osm_file_path = "/opt/airflow/logs/railway_network.osm"
    relation_id = 12351758  # 실제 사용할 관계 ID로 변경
    download_osm_data(relation_id, osm_file_path)
