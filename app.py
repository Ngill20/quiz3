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
#Add this

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
        # Extract values from form (for simplified CSV structure)
        quake_id = request.form.get('id')
        time = safe_int(request.form.get('time'))  # Assuming time is an integer like in the CSV
        latitude = safe_float(request.form.get('lat'))
        longitude = safe_float(request.form.get('long'))
        mag = safe_float(request.form.get('mag'))
        nst = safe_int(request.form.get('nst'))
        net = request.form.get('net')

        # Insert into Earthquakes table
        conn = get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO Earthquakes (
                    id, time, latitude, longitude, mag, nst, net
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, quake_id, time, latitude, longitude, mag, nst, net)
            conn.commit()
            flash('Earthquake record inserted successfully!')
        except Exception as e:
            flash(f'Insert failed: {e}')
        finally:
            cursor.close()
            conn.close()

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
        try:
            start_time_val = safe_int(request.form.get('start_time'))
            end_time_val = safe_int(request.form.get('end_time'))

            if start_time_val is None or end_time_val is None:
                flash("Please provide valid numeric time values.")
                return redirect(url_for('query2'))

            # Create cache key
            cache_key = hashlib.sha256(f"{start_time_val}_{end_time_val}".encode()).hexdigest()

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
                    SELECT id, net, time, latitude, longitude
                    FROM Earthquakes
                    WHERE time BETWEEN ? AND ?
                    ORDER BY time DESC
                """
                df = pd.read_sql(query, conn, params=[start_time_val, end_time_val])
                conn.close()
                redis_client.setex(cache_key, 300, df.to_json().encode())
                elapsed_time = time.time() - start_time

            html_table = df.to_html(classes='table table-striped', index=False).replace('\n', '')
            return render_template('results.html', tables=[html_table], titles=df.columns.values,
                                   elapsed_time=f"{elapsed_time:.4f} seconds", cache_status=cache_status)

        except Exception as e:
            flash(f"Error: {e}")
            return redirect(url_for('query2'))

    return render_template('query2.html')


@app.route('/query3', methods=['GET', 'POST'])
def query3():
    if request.method == 'POST':
        try:
            start_time_val = safe_int(request.form.get('start_time'))
            net_val = request.form.get('net', '').strip().lower()
            count = safe_int(request.form.get('count'))

            if start_time_val is None or not net_val or count is None:
                flash("Please provide valid start time, net value, and count.")
                return redirect(url_for('query3'))

            # Create Redis cache key
            cache_key = hashlib.sha256(f"{start_time_val}_{net_val}_{count}".encode()).hexdigest()

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
                    SELECT TOP (?) id, net, time, latitude, longitude
                    FROM Earthquakes
                    WHERE time >= ? AND LOWER(net) = ?
                    ORDER BY time ASC
                """
                df = pd.read_sql(query, conn, params=[count, start_time_val, net_val])
                conn.close()
                redis_client.setex(cache_key, 300, df.to_json().encode())
                elapsed_time = time.time() - start_time

            html_table = df.to_html(classes='table table-striped', index=False).replace('\n', '')
            return render_template('results.html', tables=[html_table], titles=df.columns.values,
                                   elapsed_time=f"{elapsed_time:.4f} seconds", cache_status=cache_status)

        except Exception as e:
            flash(f"Error: {e}")
            return redirect(url_for('query3'))

    return render_template('query3.html')

@app.route('/query4', methods=['GET', 'POST'])
def query4():
    if request.method == 'POST':
        try:
            t = safe_int(request.form.get('t'))
            q2_start = safe_int(request.form.get('q2_start'))
            q2_end = safe_int(request.form.get('q2_end'))

            q3_start = safe_int(request.form.get('q3_start'))
            q3_net = request.form.get('q3_net', '').strip().lower()
            q3_count = safe_int(request.form.get('q3_count'))

            if None in [t, q2_start, q2_end, q3_start, q3_count] or not q3_net:
                flash("Please enter all required values.")
                return redirect(url_for('query4'))

            q2_times = []
            q3_times = []
            last_q2_df = None
            last_q3_df = None

            conn = get_connection()

            for _ in range(t):
                start = time.time()
                q2_query = """
                    SELECT id, net, time, latitude, longitude
                    FROM Earthquakes
                    WHERE time BETWEEN ? AND ?
                    ORDER BY time DESC
                """
                last_q2_df = pd.read_sql(q2_query, conn, params=[q2_start, q2_end])
                q2_times.append(time.time() - start)

                start = time.time()
                q3_query = """
                    SELECT TOP (?) id, net, time, latitude, longitude
                    FROM Earthquakes
                    WHERE time >= ? AND LOWER(net) = ?
                    ORDER BY time ASC
                """
                last_q3_df = pd.read_sql(q3_query, conn, params=[q3_count, q3_start, q3_net])
                q3_times.append(time.time() - start)

            conn.close()

            q2_total = sum(q2_times)
            q3_total = sum(q3_times)

            # Convert dataframes to HTML tables
            q2_table = last_q2_df.to_html(classes='table table-striped', index=False).replace('\n', '') if last_q2_df is not None else ""
            q3_table = last_q3_df.to_html(classes='table table-striped', index=False).replace('\n', '') if last_q3_df is not None else ""

            return render_template("query4_results.html",
                                   t=t,
                                   q2_times=q2_times,
                                   q3_times=q3_times,
                                   q2_total=q2_total,
                                   q3_total=q3_total,
                                   q2_table=q2_table,
                                   q3_table=q3_table)

        except Exception as e:
            flash(f"Error: {e}")
            return redirect(url_for('query4'))

    return render_template('query4.html')

@app.route('/query5', methods=['GET', 'POST'])
def query5():
    if request.method == 'POST':
        try:
            t = safe_int(request.form.get('t'))
            q2_start = safe_int(request.form.get('q2_start'))
            q2_end = safe_int(request.form.get('q2_end'))

            q3_start = safe_int(request.form.get('q3_start'))
            q3_net = request.form.get('q3_net', '').strip().lower()
            q3_count = safe_int(request.form.get('q3_count'))

            if None in [t, q2_start, q2_end, q3_start, q3_count] or not q3_net:
                flash("Please enter all required values.")
                return redirect(url_for('query5'))

            q2_times, q3_times = [], []
            q2_hits, q2_misses = 0, 0
            q3_hits, q3_misses = 0, 0
            last_q2_df, last_q3_df = None, None

            for _ in range(t):
                # --- QUERY 2 ---
                q2_key = hashlib.sha256(f"q2_{q2_start}_{q2_end}".encode()).hexdigest()
                start = time.time()
                cached_q2 = redis_client.get(q2_key)

                if cached_q2:
                    last_q2_df = pd.read_json(io.BytesIO(cached_q2))
                    q2_hits += 1
                else:
                    conn = get_connection()
                    query = """
                        SELECT id, net, time, latitude, longitude
                        FROM Earthquakes
                        WHERE time BETWEEN ? AND ?
                        ORDER BY time DESC
                    """
                    last_q2_df = pd.read_sql(query, conn, params=[q2_start, q2_end])
                    conn.close()
                    redis_client.setex(q2_key, 300, last_q2_df.to_json().encode())
                    q2_misses += 1
                q2_times.append(time.time() - start)

                # --- QUERY 3 ---
                q3_key = hashlib.sha256(f"q3_{q3_start}_{q3_net}_{q3_count}".encode()).hexdigest()
                start = time.time()
                cached_q3 = redis_client.get(q3_key)

                if cached_q3:
                    last_q3_df = pd.read_json(io.BytesIO(cached_q3))
                    q3_hits += 1
                else:
                    conn = get_connection()
                    query = """
                        SELECT TOP (?) id, net, time, latitude, longitude
                        FROM Earthquakes
                        WHERE time >= ? AND LOWER(net) = ?
                        ORDER BY time ASC
                    """
                    last_q3_df = pd.read_sql(query, conn, params=[q3_count, q3_start, q3_net])
                    conn.close()
                    redis_client.setex(q3_key, 300, last_q3_df.to_json().encode())
                    q3_misses += 1
                q3_times.append(time.time() - start)

            q2_total = sum(q2_times)
            q3_total = sum(q3_times)

            return render_template("query5_results.html",
                                   t=t,
                                   q2_times=q2_times,
                                   q3_times=q3_times,
                                   q2_total=q2_total,
                                   q3_total=q3_total,
                                   q2_hits=q2_hits,
                                   q2_misses=q2_misses,
                                   q3_hits=q3_hits,
                                   q3_misses=q3_misses,
                                   q2_table=last_q2_df.to_html(classes='table table-striped', index=False).replace('\n', '') if last_q2_df is not None else "",
                                   q3_table=last_q3_df.to_html(classes='table table-striped', index=False).replace('\n', '') if last_q3_df is not None else "")

        except Exception as e:
            flash(f"Error: {e}")
            return redirect(url_for('query5'))

    return render_template('query5.html')

@app.route('/query6', methods=['GET', 'POST'])
def query6():
    if request.method == 'POST':
        try:
            old_time = safe_int(request.form.get('old_time'))
            if old_time is None:
                flash("Please enter a valid existing time value.")
                return redirect(url_for('query6'))

            # Collect new values
            update_fields = {
                'time': safe_int(request.form.get('new_time')),
                'latitude': safe_float(request.form.get('lat')),
                'longitude': safe_float(request.form.get('long')),
                'mag': safe_float(request.form.get('mag')),
                'nst': safe_int(request.form.get('nst')),
                'net': request.form.get('net', '').strip() or None,
                'id': request.form.get('id', '').strip() or None
            }

            update_fields = {k: v for k, v in update_fields.items() if v is not None and v != ''}

            if not update_fields:
                flash("No new values provided for update.")
                return redirect(url_for('query6'))

            # Build SQL update statement
            set_clause = ", ".join([f"{field} = ?" for field in update_fields])
            values = list(update_fields.values()) + [old_time]

            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute(f"UPDATE Earthquakes SET {set_clause} WHERE time = ?", values)
            conn.commit()
            affected = cursor.rowcount

            updated_df = None
            if affected > 0:
                # Fetch the updated record (use new time if it was changed)
                final_time = update_fields.get('time', old_time)
                updated_df = pd.read_sql("SELECT * FROM Earthquakes WHERE time = ?", conn, params=[final_time])

            cursor.close()
            conn.close()

            if affected == 0:
                flash("No records found with the provided time.")
                return redirect(url_for('query6'))
            else:
                flash(f"{affected} record(s) updated successfully.")
                updated_table = updated_df.to_html(classes='table table-striped', index=False).replace('\n', '')
                return render_template("query6.html", updated_table=updated_table)

        except Exception as e:
            flash(f"Error: {e}")
            return redirect(url_for('query6'))

    return render_template("query6.html")



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