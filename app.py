# app.py
from flask import Flask, render_template, request, jsonify, send_file
import psycopg2
import geopandas as gpd
from sqlalchemy import create_engine
import os
from datetime import datetime
import warnings
import urllib.parse

warnings.filterwarnings('ignore')

app = Flask(__name__)

# Use Railway's provided PORT and temporary directory
app.config['UPLOAD_FOLDER'] = '/tmp/uploads'
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# Railway provides DATABASE_URL like: postgres://user:pass@host:port/db
DATABASE_URL = os.environ.get('DATABASE_URL')
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

def get_sqlalchemy_engine():
    """Create SQLAlchemy engine from DATABASE_URL"""
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL not set")
    return create_engine(DATABASE_URL)

def get_connection():
    """Create psycopg2 connection"""
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL not set")
    return psycopg2.connect(DATABASE_URL)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/databases', methods=['GET'])
def get_databases():
    """Railway has only one DB, so return its name"""
    try:
        # Extract database name from DATABASE_URL
        parsed = urllib.parse.urlparse(DATABASE_URL)
        db_name = parsed.path[1:]  # remove leading /
        return jsonify({'databases': [db_name]})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/tables', methods=['POST'])
def get_tables():
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT f_table_name, f_geometry_column, type
            FROM geometry_columns
            WHERE f_table_schema = 'public'
            ORDER BY f_table_name;
        """)
        tables = [{'name': row[0], 'geom_col': row[1], 'geom_type': row[2]} 
                  for row in cur.fetchall()]
        cur.close()
        conn.close()
        return jsonify({'tables': tables})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/analyze', methods=['POST'])
def analyze():
    try:
        data = request.json
        line_table = data.get('line_table')
        point_table = data.get('point_table')
        buffer_size = float(data.get('buffer_size', 30))
        tier_column = data.get('tier_column', 'severity')

        if not all([line_table, point_table]):
            return jsonify({'error': 'Missing required parameters'}), 400

        engine = get_sqlalchemy_engine()

        # Get geometry columns
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT f_table_name, f_geometry_column
            FROM geometry_columns
            WHERE f_table_schema = 'public'
            AND f_table_name IN (%s, %s);
        """, (line_table, point_table))
        geom_info = {row[0]: row[1] for row in cur.fetchall()}
        cur.close()
        conn.close()

        if line_table not in geom_info or point_table not in geom_info:
            return jsonify({'error': 'Geometry columns not found'}), 400

        line_geom_col = geom_info[line_table]
        point_geom_col = geom_info[point_table]

        # Read data
        lines = gpd.read_postgis(f"SELECT * FROM {line_table}", engine, geom_col=line_geom_col)
        points = gpd.read_postgis(f"SELECT * FROM {point_table}", engine, geom_col=point_geom_col)

        if lines.empty or points.empty:
            return jsonify({'error': 'One or both tables are empty'}), 400

        # Reproject if needed
        if lines.crs != points.crs:
            points = points.to_crs(lines.crs)

        # Project to metric CRS for buffering
        if lines.crs.is_geographic:
            centroid = lines.unary_union.centroid
            lon, lat, lon = centroid.y, centroid.x
            utm_zone = int((lon + 180) / 6) + 1
            hemisphere = 'north' if lat >= 0 else 'south'
            utm_crs = f"EPSG:32{'6' if hemisphere == 'north' else '7'}{utm_zone:02d}"
            try:
                lines_proj = lines.to_crs(utm_crs)
                points_proj = points.to_crs(utm_crs)
            except:
                # Fallback to custom proj string
                utm_crs = f"+proj=utm +zone={utm_zone} +{'north' if hemisphere == 'north' else 'south'} +datum=WGS84 +units=m"
                lines_proj = lines.to_crs(utm_crs)
                points_proj = points.to_crs(utm_crs)
        else:
            lines_proj = lines.copy()
            points_proj = points.copy()

        # Buffer and spatial join
        lines_proj['buffer'] = lines_proj.geometry.buffer(buffer_size)
        all_buffers = lines_proj['buffer'].unary_union
        points_in_buffer = points_proj[points_proj.geometry.within(all_buffers)].copy()

        total_in_buffer = len(points_in_buffer)

        # Tier counts
        tier_col_found = None
        for col in points_in_buffer.columns:
            if col.lower() == tier_column.lower():
                tier_col_found = col
                break

        tier_counts = {}
        if tier_col_found and total_in_buffer > 0:
            counts = points_in_buffer[tier_col_found].value_counts()
            tier_counts = {str(k): int(v) for k, v in counts.items()}

        # Per-line results
        results_by_line = []
        for idx, row in lines_proj.iterrows():
            buf = row['buffer']
            pts = points_proj[points_proj.geometry.within(buf)]
            if len(pts) == 0:
                continue
            line_id = row.get('gid') or row.get('id') or idx
            res = {"line_id": line_id, "total_points": len(pts)}
            if tier_col_found:
                tc = pts[tier_col_found].value_counts().to_dict()
                res["tier_counts"] = {str(k): int(v) for k, v in tc.items()}
            results_by_line.append(res)

        # Save outputs to /tmp (Railway allows writing here)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_file = f'points_{timestamp}.csv'
        gpkg_file = f'analysis_{timestamp}.gpkg'

        csv_path = os.path.join(app.config['UPLOAD_FOLDER'], csv_file)
        gpkg_path = os.path.join(app.config['UPLOAD_FOLDER'], gpkg_file)

        # CSV
        points_in_buffer.drop(columns=['geometry'], errors='ignore').to_csv(csv_path, index=False)

        # GeoPackage
        orig_crs = lines.crs
        lines.to_file(gpkg_path, layer='original_lines', driver='GPKG')
        points.to_file(gpkg_path, layer='all_points', driver='GPKG')
        points_in_buffer.to_crs(orig_crs).to_file(gpkg_path, layer='points_in_buffer', driver='GPKG')
        gpd.GeoDataFrame({'buffer_m': [buffer_size]}, geometry=lines_proj['buffer'], crs=lines_proj.crs)\
           .to_crs(orig_crs).to_file(gpkg_path, layer='buffers', driver='GPKG')

        return jsonify({
            'success': True,
            'total_points_in_buffer': total_in_buffer,
            'total_points': len(points),
            'line_count': len(lines),
            'buffer_size': buffer_size,
            'tier_column': tier_col_found or "(not found)",
            'overall_tier_counts': tier_counts,
            'results_by_line': results_by_line,
            'csv_download': csv_file,
            'gpkg_download': gpkg_file
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/download/<filename>')
def download_file(filename):
    file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    if not os.path.exists(file_path):
        return jsonify({'error': 'File not found'}), 404
    return send_file(
        file_path,
        as_attachment=True,
        download_name=filename,
        mimetype='application/geopackage+sqlite3' if filename.endswith('.gpkg') else 'text/csv'
    )

if __name__ == '__main__':
    # Railway uses PORT env var
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
