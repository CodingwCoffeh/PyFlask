from flask import Flask, render_template, request, jsonify, send_file
import psycopg2
import geopandas as gpd
from sqlalchemy import create_engine
import tempfile
import os
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

def get_sqlalchemy_engine(database):
    """Create SQLAlchemy engine for GeoPandas"""
    return create_engine(f'postgresql://postgres:postgre@https://pyflask-production.up.railway.app/{database}')

def get_connection(database):
    """Create database connection"""
    return psycopg2.connect(
        host="https://pyflask-production.up.railway.app",
        database=database,
        user="postgres",
        password="postgre"
    )

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/databases', methods=['GET'])
def get_databases():
    """Get list of all databases"""
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            password="postgre"
        )
        cur = conn.cursor()
        cur.execute("""
            SELECT datname FROM pg_database 
            WHERE datistemplate = false 
            ORDER BY datname;
        """)
        databases = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()
        return jsonify({'databases': databases})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/tables', methods=['POST'])
def get_tables():
    """Get all spatial tables from database"""
    try:
        database = request.json.get('database')
        if not database:
            return jsonify({'error': 'Database name required'}), 400
        
        conn = get_connection(database)
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
    """Run buffer analysis using GeoPandas"""
    try:
        data = request.json
        database = data.get('database')
        line_table = data.get('line_table')
        point_table = data.get('point_table')
        buffer_size = float(data.get('buffer_size', 30))
        tier_column = data.get('tier_column', 'severity')
        
        if not all([database, line_table, point_table]):
            return jsonify({'error': 'Missing required parameters'}), 400
        
        print(f"Analyzing: {database} | Lines: {line_table} | Points: {point_table} | Buffer: {buffer_size}m")
        
        # Create SQLAlchemy engine
        engine = get_sqlalchemy_engine(database)
        
        # Get geometry column names first
        conn = get_connection(database)
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
            return jsonify({'error': 'Could not find geometry columns for tables'}), 400
        
        line_geom_col = geom_info[line_table]
        point_geom_col = geom_info[point_table]
        
        print(f"Geometry columns - Line: {line_geom_col}, Point: {point_geom_col}")
        
        # Read layers with GeoPandas
        lines = gpd.read_postgis(f"SELECT * FROM {line_table}", engine, geom_col=line_geom_col)
        points = gpd.read_postgis(f"SELECT * FROM {point_table}", engine, geom_col=point_geom_col)
        
        print(f"Loaded {len(lines)} lines and {len(points)} points")
        
        # Check CRS and reproject if needed
        if lines.crs != points.crs:
            points = points.to_crs(lines.crs)
            print("Reprojected points to match lines")
        
        # Reproject to UTM if in lat/lon for metric buffering
        if lines.crs.is_geographic:
            centroid = lines.unary_union.centroid
            lon, lat = centroid.x, centroid.y
            utm_zone = int((lon + 180) / 6) + 1
            hemisphere = 'north' if lat >= 0 else 'south'
            utm_crs = f"+proj=utm +zone={utm_zone} +{hemisphere} +datum=WGS84 +units=m +no_defs"
            
            lines_proj = lines.to_crs(utm_crs)
            points_proj = points.to_crs(utm_crs)
            print(f"Reprojected to UTM Zone {utm_zone}{hemisphere[0].upper()}")
        else:
            lines_proj = lines.copy()
            points_proj = points.copy()
        
        # Create buffers
        lines_proj['buffer'] = lines_proj.geometry.buffer(buffer_size)
        
        # Find unique points in ANY buffer
        all_buffers = lines_proj['buffer'].unary_union
        points_in_buffer = points_proj[points_proj.geometry.within(all_buffers)].copy()
        
        total_points = len(points_in_buffer)
        print(f"Found {total_points} points in buffer")
        
        # Check if tier column exists
        tier_col_found = None
        for col in [tier_column, tier_column.lower(), tier_column.upper(), tier_column.capitalize()]:
            if col in points_in_buffer.columns:
                tier_col_found = col
                break
        
        # Count by tier
        tier_counts = {}
        if tier_col_found and total_points > 0:
            counts = points_in_buffer[tier_col_found].value_counts()
            tier_counts = {str(k): int(v) for k, v in counts.items()}
        
        # Per-line statistics
        results_by_line = []
        for idx, line_row in lines_proj.iterrows():
            buffer_geom = line_row['buffer']
            points_in_line = points_proj[points_proj.geometry.within(buffer_geom)]
            count = len(points_in_line)
            
            if count == 0:
                continue
            
            # Get line ID
            line_id = idx + 1
            if 'gid' in line_row.index:
                line_id = line_row['gid']
            elif 'id' in line_row.index:
                line_id = line_row['id']
            
            line_result = {
                "line_id": line_id,
                "total_points": count
            }
            
            if tier_col_found and count > 0:
                tier_counts_line = points_in_line[tier_col_found].value_counts().to_dict()
                line_result["tier_counts"] = {str(k): int(v) for k, v in tier_counts_line.items()}
            
            results_by_line.append(line_result)
        
        # Store data for download
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Save points to CSV
        csv_filename = f'points_in_buffer_{timestamp}.csv'
        csv_path = os.path.join(app.config['UPLOAD_FOLDER'], csv_filename)
        
        # Drop geometry column (use actual column name)
        geom_col_name = points_in_buffer.geometry.name
        points_csv = points_in_buffer.drop(columns=[geom_col_name])
        points_csv.to_csv(csv_path, index=False)
        
        # Save to GeoPackage
        gpkg_filename = f'analysis_{timestamp}.gpkg'
        gpkg_path = os.path.join(app.config['UPLOAD_FOLDER'], gpkg_filename)
        
        # Reproject back to original CRS for output
        original_crs = lines.crs
        
        # Layer 1: Original lines
        lines.to_file(gpkg_path, driver='GPKG', layer='original_lines')
        
        # Layer 2: All points
        points.to_file(gpkg_path, driver='GPKG', layer='all_points')
        
        # Layer 3: Points in buffer
        points_in_buffer_orig = points_in_buffer.to_crs(original_crs)
        points_in_buffer_orig.to_file(gpkg_path, driver='GPKG', layer='points_in_buffer')
        
        # Layer 4: Buffer polygons
        buffer_gdf = gpd.GeoDataFrame(
            {'line_id': range(len(lines_proj)), 'buffer_m': buffer_size},
            geometry=lines_proj['buffer'].values,
            crs=lines_proj.crs
        )
        buffer_gdf_orig = buffer_gdf.to_crs(original_crs)
        buffer_gdf_orig.to_file(gpkg_path, driver='GPKG', layer='buffers')
        
        return jsonify({
            'success': True,
            'total_points_in_buffer': total_points,
            'total_points': len(points),
            'line_count': len(lines),
            'buffer_size': buffer_size,
            'tier_column': tier_col_found if tier_col_found else f"{tier_column} (not found)",
            'overall_tier_counts': tier_counts,
            'results_by_line': results_by_line,
            'csv_download': csv_filename,
            'gpkg_download': gpkg_filename
        })
        
    except Exception as e:
        print(f"ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/download/<filename>')
def download_file(filename):
    """Download generated file"""
    try:
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        if not os.path.exists(file_path):
            return jsonify({'error': 'File not found'}), 404
        
        if filename.endswith('.csv'):
            mimetype = 'text/csv'
        elif filename.endswith('.gpkg'):
            mimetype = 'application/geopackage+sqlite3'
        else:
            mimetype = 'application/octet-stream'
        
        return send_file(file_path, as_attachment=True, download_name=filename, mimetype=mimetype)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)



