import geojson
import osmium as osm
import os

class OSMHandler(osm.SimpleHandler):
    def __init__(self):
        super().__init__()
        self.nodes = {}
        self.ways = []

    def node(self, n):
        """Store the coordinates of the node."""
        self.nodes[n.id] = (n.location.lon, n.location.lat)

    def way(self, w):
        """Convert railway ways to GeoJSON format."""
        if 'railway' in w.tags and w.tags['railway'] == 'rail':
            coords = [(self.nodes[n.ref][0], self.nodes[n.ref][1]) for n in w.nodes if n.ref in self.nodes]
            if coords:
                feature = geojson.Feature(
                    type="Feature",
                    geometry=geojson.LineString(coords),
                    properties=dict(w.tags)
                )
                self.ways.append(feature)

def convert_osm_to_geojson(osm_file, output_file):
    """Convert OSM data to GeoJSON format and save to file."""
    osmhandler = OSMHandler()
    osmhandler.apply_file(osm_file)

    geojson_data = {
        "type": "FeatureCollection",
        "features": osmhandler.ways
    }

    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, 'w', encoding='utf-8') as f:
        geojson.dump(geojson_data, f, ensure_ascii=False, indent=2)
        print(f"GeoJSON data saved to: {output_file}")

if __name__ == "__main__":
    # 컨테이너 내부 경로 설정
    osm_file_path = "/opt/airflow/logs/railway_network.osm"
    geojson_file_path = "/opt/airflow/logs/railway_network.geojson"

    convert_osm_to_geojson(osm_file_path, geojson_file_path)
