import folium
import geojson
import os

def create_map(geojson_file, output_file):
    """Create a map with Folium from a GeoJSON file and save it as an HTML file."""
    # GeoJSON 파일 로드
    try:
        with open(geojson_file, 'r', encoding='utf-8') as f:
            geojson_data = geojson.load(f)
    except FileNotFoundError:
        print(f"파일을 찾을 수 없습니다: {geojson_file}")
        return
    except Exception as e:
        print(f"파일을 로드하는 동안 오류 발생: {e}")
        return

    # Folium 지도 생성
    m = folium.Map(location=[37.12, 127.72], zoom_start=11)

    # GeoJSON 데이터를 지도에 추가
    folium.GeoJson(
        data=geojson_data,
        style_function=lambda feature: {
            'color': 'blue',
            'weight': 4,
            'opacity': 0.7
        }
    ).add_to(m)

    # HTML 파일로 저장
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    m.save(output_file)
    print(f"지도 생성 완료! {output_file} 파일을 열어보세요.")

if __name__ == "__main__":
    # 컨테이너 내부 경로 설정
    geojson_file = "/opt/airflow/logs/railway_network.geojson"
    output_file = "/opt/airflow/logs/osmgeojson_html_map.html"

    # 지도 생성 함수 호출
    create_map(geojson_file, output_file)
