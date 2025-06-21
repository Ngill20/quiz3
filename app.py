from flask import Flask, render_template, request, redirect, url_for, flash
import pandas as pd
from datetime import datetime
import pyodbc
from dotenv import load_dotenv
import os
import math
import redis
import hashlib
import time
import io

app = Flask(__name__)
app.secret_key = 'supersecret'  # for flash messages

load_dotenv()
password = os.getenv('SQL_PASSWORD')


server = 'quiz3server.database.windows.net'
database = 'quiz3db' 
username = 'quiz3user'
driver = '{ODBC Driver 18 for SQL Server}'

#Redis Configuration
redis_client = redis.StrictRedis(
    host=os.getenv("REDIS_HOST"),
    port=6380,
    db=0,
    password=os.getenv("REDIS_KEY"),
    ssl=True
)


def get_connection():
    return pyodbc.connect(
        f'DRIVER={driver};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'UID={username};'
        f'PWD={password};'
        f'Encrypt=yes;'
        f'TrustServerCertificate=no;'
        f'Connection Timeout=30;'
    )

@app.route('/')
def index():
    print("Rendering index.html")  # Make sure this prints in your terminal
    return render_template('index.html')
    #return "<h1>Test page</h1>"

@app.route('/insert', methods=['GET', 'POST'])
def insert():
    if request.method == 'POST':
        # Extract values from form (with default fallback)
        quake_id = request.form.get('id')
        time = safe_datetime(request.form.get('time'))
        latitude = safe_float(request.form.get('latitude'))
        longitude = safe_float(request.form.get('longitude'))
        depth = safe_float(request.form.get('depth'))
        mag = safe_float(request.form.get('mag'))
        magType = request.form.get('magType')
        nst = safe_int(request.form.get('nst'))
        gap = safe_float(request.form.get('gap'))
        dmin = safe_float(request.form.get('dmin'))
        rms = safe_float(request.form.get('rms'))
        net = request.form.get('net')
        updated = safe_datetime(request.form.get('updated'))
        place = request.form.get('place')
        type_ = request.form.get('type')
        horizontalError = safe_float(request.form.get('horizontalError'))
        depthError = safe_float(request.form.get('depthError'))
        magError = safe_float(request.form.get('magError'))
        magNst = safe_int(request.form.get('magNst'))
        status = request.form.get('status')
        locationSource = request.form.get('locationSource')
        magSource = request.form.get('magSource')

        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO Earthquakes (
                id, time, latitude, longitude, depth, mag, magType, nst, gap, dmin, rms,
                net, updated, place, type, horizontalError, depthError, magError,
                magNst, status, locationSource, magSource
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, quake_id, time, latitude, longitude, depth, mag, magType, nst, gap, dmin, rms,
             net, updated, place, type_, horizontalError, depthError, magError,
             magNst, status, locationSource, magSource)

        conn.commit()
        cursor.close()
        conn.close()
        flash('Earthquake record inserted successfully!')
        return redirect(url_for('index'))
    
    return render_template('insert.html')

@app.route('/query', methods=['GET', 'POST'])
def query():
    if request.method == 'POST':
        min_mag = request.form.get('min_mag', 0)
        max_mag = request.form.get('max_mag', 10)

        #Create cache key based on query
        cache_key = hashlib.sha256(f"{min_mag}_{max_mag}".encode()).hexdigest()

        start_time = time.time()

        cached_result = redis_client.get(cache_key)

        if cached_result:
            df = pd.read_json(io.BytesIO(cached_result))
            elapsed_time = time.time() - start_time
            cache_status = "CACHE HIT"
            print(cache_status)
        else:
            cache_status = "CACHE MISS"
            print(cache_status)
            conn = get_connection()
            query = """
                SELECT id, time, latitude, longitude, mag, nst, net
                FROM Earthquakes
                WHERE mag BETWEEN ? AND ?
                ORDER BY time DESC
            """
            df = pd.read_sql(query, conn, params=[min_mag, max_mag])
            conn.close()
            #cache this result into reddis for 5 mins
            redis_client.setex(cache_key,300, df.to_json().encode())
            elapsed_time = time.time() - start_time

        # Remove leading/trailing \n and whitespace in the HTML
        html_table = df.to_html(classes='table table-striped', index=False).replace('\n', '')

        return render_template('results.html', tables=[html_table], titles=df.columns.values, elapsed_time=f"{elapsed_time:.4f} seconds",
            cache_status=cache_status)
    
    return render_template('query.html')


@app.route('/query2', methods=['GET', 'POST'])
def query2():
    if request.method == 'POST':
        start_date = request.form.get('start_date')
        end_date = request.form.get('end_date')
        min_mag = request.form.get('min_mag', 0)
        max_mag = request.form.get('max_mag', 10)

        conn = get_connection()
        query = """
            SELECT id, time, latitude, longitude, mag, nst, net
            FROM Earthquakes
            WHERE mag BETWEEN ? AND ?
              AND time BETWEEN ? AND ?
            ORDER BY time DESC
        """
        df = pd.read_sql(query, conn, params=[min_mag, max_mag, start_date, end_date])
        conn.close()

        html_table = df.to_html(classes='table table-striped', index=False).replace('\n', '')
        return render_template('results.html', tables=[html_table], titles=df.columns.values)

    return render_template('query2.html')

@app.route('/query3', methods=['GET', 'POST'])
def query3():
    if request.method == 'POST':
        try:
            center_lat = float(request.form.get('latitude'))
            center_lon = float(request.form.get('longitude'))
            radius_km = float(request.form.get('distance_km'))

            # Create Redis cache key
            cache_key = hashlib.sha256(f"{center_lat}_{center_lon}_{radius_km}".encode()).hexdigest()

            start_time = time.time()
            cached_result = redis_client.get(cache_key)

            if cached_result:
                df = pd.read_json(io.BytesIO(cached_result))
                elapsed_time = time.time() - start_time
                cache_status = "CACHE HIT"
                print(cache_status)
            else:
                cache_status = "CACHE MISS"
                print(cache_status)
                # Approximate conversions
                delta_lat = radius_km / 111  # 1 deg latitude ≈ 111 km
                delta_lon = radius_km / (111 * abs(math.cos(math.radians(center_lat))) + 1e-6)  # avoid div by zero

                min_lat = center_lat - delta_lat
                max_lat = center_lat + delta_lat
                min_lon = center_lon - delta_lon
                max_lon = center_lon + delta_lon

                conn = get_connection()
                query = """
                    SELECT id, time, latitude, longitude, mag, nst, net
                    FROM Earthquakes
                    WHERE latitude BETWEEN ? AND ?
                    AND longitude BETWEEN ? AND ?
                    ORDER BY time DESC
                """
                df = pd.read_sql(query, conn, params=[min_lat, max_lat, min_lon, max_lon])
                conn.close()

                # Cache for 5 minutes
                redis_client.setex(cache_key, 300, df.to_json().encode())
                elapsed_time = time.time() - start_time

            html_table = df.to_html(classes='table table-striped', index=False).replace('\n', '')
            return render_template('results.html', tables=[html_table], titles=df.columns.values,
                                   elapsed_time=f"{elapsed_time:.4f} seconds", cache_status=cache_status)

        except Exception as e:
            flash(f"Error: {e}")
            return redirect(url_for('query3'))

    return render_template('query3.html')


@app.route('/upload', methods=['GET', 'POST'])
def upload():
    if request.method == 'POST':
        file = request.files['file']
        if file and file.filename.endswith('.csv'):
            file_path = os.path.join('static', 'uploads', file.filename)
            file.save(file_path)

            # Read CSV with expected columns
            df_cleaned = pd.read_csv(file_path, skip_blank_lines=True)

            conn = get_connection()
            cursor = conn.cursor()
            for index, row in df_cleaned.iterrows():
                try:
                    time      = safe_int(row['time'])
                    latitude  = safe_float(row['lat'])
                    longitude = safe_float(row['long'])
                    mag       = safe_float(row['mag'])
                    nst       = safe_int(row['nst'])
                    net       = str(row['net']) if pd.notna(row['net']) else None
                    id_       = str(row['id'])
                    
                    print(f"Row {index}: time={time}, lat={latitude}, mag={mag}, id={id_}")
                    cursor.execute("""
                        INSERT INTO Earthquakes (
                            time, latitude, longitude, mag, nst, net, id
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, time, latitude, longitude, mag, nst, net, id_)

                except Exception as e:
                    print(f"Failed to insert row {index}: {e}")
                    print(df_cleaned.iloc[index])

            conn.commit()
            cursor.close()
            conn.close()
            flash('CSV data uploaded successfully!')
            return redirect(url_for('index'))
        else:
            flash('Please upload a valid CSV file.')
            return redirect(url_for('upload'))
    return render_template('upload.html')



def safe_float(val):
    try:
        val = str(val).strip()
        if val.lower() in ["", "nan", "null", "none"]:
            return None
        return float(val)
    except (ValueError, TypeError):
        return None


def safe_int(val):
    try:
        val = str(val).strip()
        if val.lower() in ["", "nan", "null", "none"]:
            return None
        return int(float(val))  # int("3.0") fails, but int(float("3.0")) works
    except (ValueError, TypeError):
        return None

def safe_datetime(val):
    try:
        return pd.to_datetime(val)
    except Exception:
        return None
    
if __name__ == '__main__':
    os.makedirs(os.path.join('static', 'uploads'), exist_ok=True)
    app.run(debug=True)