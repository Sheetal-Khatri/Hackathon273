from flask import Flask, request, jsonify
import requests
import os
import csv
from datetime import date, datetime
import mysql.connector
from mysql.connector import Error

app = Flask(__name__)

# CDEC reservoir info
reservoirs = {
    "Shasta_Lake": "SHA",
    "Lake_Oroville": "ORO",
    "Trinity_Lake": "CLE",
    "New_Melones_Lake": "NML",
    "San_Luis_Reservoir": "SNL",
    "Don_Pedro_Reservoir": "DNP",
    "Lake_Berryessa": "BER",
    "Folsom_Lake": "FOL",
    "New_Bullards_Bar_Reservoir": "BUL",
    "Pine_Flat_Lake": "PNF"
}

BASE_URL = "https://cdec.water.ca.gov/dynamicapp/req/CSVDataServlet"
CSV_FOLDER = "csv_data"
os.makedirs(CSV_FOLDER, exist_ok=True)

# MySQL DB config (set these in your environment)
DB_HOST = os.environ.get("DB_HOST", "reservoir-db.c1sk2imgwsr1.us-west-1.rds.amazonaws.com")
DB_PORT = int(os.environ.get("DB_PORT", 3306))
DB_USER = os.environ.get("DB_USER", "admin")
DB_PASS = os.environ.get("DB_PASS", "hackathoncmpe273")
DB_NAME = os.environ.get("DB_NAME", "reservoir")


def get_db_connection():
    """Establish a connection to the MySQL database."""
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME
        )
        return connection
    except Error as e:
        print("Error connecting to MySQL:", e)
        return None


def create_table_if_not_exists():
    """Create the reservoir_master_data table if it doesn't exist."""
    connection = get_db_connection()
    if connection is None:
        print("DB connection not available.")
        return
    create_table_query = """
    CREATE TABLE IF NOT EXISTS reservoir_master_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        reservoir VARCHAR(100),
        value FLOAT,
        units VARCHAR(50),
        station_id VARCHAR(50),
        sensor_type VARCHAR(50),
        sensor_number VARCHAR(50),
        obs_date DATE,
        duration VARCHAR(50),
        date_time DATETIME,
        data_flag VARCHAR(50)
    );
    """
    cursor = connection.cursor()
    try:
        cursor.execute(create_table_query)
        connection.commit()
    except Error as e:
        print("Error creating table:", e)
    finally:
        cursor.close()
        connection.close()


def try_parse_date(date_str, formats):
    """Try to parse a date string using a list of formats."""
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None


def filter_csv_data(filepath):
    """
    Read the CSV file and filter data by converting invalid or missing values to None.
    Expects CSV headers:
    STATION_ID, DURATION, SENSOR_NUMBER, SENSOR_TYPE, DATE TIME, OBS DATE, VALUE, DATA_FLAG, UNITS
    Returns a list of tuples in the form:
    (value, units, station_id, sensor_type, sensor_number, obs_date, duration, date_time, data_flag)
    """
    filtered_data = []
    try:
        with open(filepath, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                # Use the CSV header keys and assign None if missing.
                station_id    = row.get("STATION_ID", "").strip() or None
                duration      = row.get("DURATION", "").strip() or None
                sensor_number = row.get("SENSOR_NUMBER", "").strip() or None
                sensor_type   = row.get("SENSOR_TYPE", "").strip() or None
                date_time_str = row.get("DATE TIME", "").strip() or None  # Note the space in the header.
                obs_date_str  = row.get("OBS DATE", "").strip() or None   # Note the space in the header.
                value_str     = row.get("VALUE", "").strip() or None
                data_flag     = row.get("DATA_FLAG", "").strip() or None
                units         = row.get("UNITS", "").strip() or None

                # For VALUE: if it is "na", "n/a", or "unavailable", treat it as missing.
                if value_str and value_str.lower() in ["na", "n/a", "unavailable"]:
                    value_str = None

                # Convert VALUE to float if possible.
                if value_str:
                    value_str = value_str.replace(",", "")
                    try:
                        value = float(value_str)
                    except ValueError:
                        value = None
                else:
                    value = None

                # Parse OBS DATE; try multiple formats.
                if obs_date_str:
                    obs_date_dt = try_parse_date(obs_date_str, ["%Y-%m-%d", "%m/%d/%Y"])
                    obs_date = obs_date_dt.date() if obs_date_dt else None
                else:
                    obs_date = None

                # Parse DATE TIME; try multiple formats including "20240101 0000".
                if date_time_str:
                    dt = try_parse_date(date_time_str, ["%Y-%m-%d %H:%M:%S",
                                                         "%m/%d/%Y %H:%M:%S",
                                                         "%Y%m%d %H%M"])
                    if dt:
                        # Convert to a datetime with time set to midnight.
                        date_time_val = datetime.combine(dt.date(), datetime.min.time())
                    else:
                        date_time_val = None
                else:
                    date_time_val = None

                filtered_data.append(
                    (value, units, station_id, sensor_type, sensor_number, obs_date, duration, date_time_val, data_flag)
                )
    except Exception as e:
        print(f"Error processing file {filepath}: {e}")
    return filtered_data


def store_data_in_db(data):
    """
    Insert the filtered data into the reservoir_master_data table.
    Data should be a list of tuples in the form:
    (value, units, station_id, sensor_type, sensor_number, obs_date, duration, date_time, data_flag)
    This function inserts exactly these 9 fields.
    """
    connection = get_db_connection()
    if connection is None:
        print("DB connection not available.")
        return "DB connection error"
    insert_query = """
    INSERT INTO reservoir_master_data 
        (VALUE, UNITS, STATION_ID, SENSOR_TYPE, SENSOR_NUMBER, OBS_DATE, DURATION, DATE_TIME, DATA_FLAG)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor = connection.cursor()
    rows_inserted = 0
    try:
        for row in data:
            row = tuple(row)
            if len(row) < 9:
                # Pad with None if there are fewer than 9 fields.
                row = row + (None,) * (9 - len(row))
            elif len(row) > 9:
                # Truncate if there are more than 9 fields.
                row = row[:9]
            cursor.execute(insert_query, row)
            rows_inserted += 1
        connection.commit()
        return f"Inserted {rows_inserted} rows"
    except Error as e:
        print("Error inserting data:", e)
        connection.rollback()
        return f"Error: {str(e)}"
    finally:
        cursor.close()
        connection.close()


@app.route('/api/fetch-data', methods=['GET'])
def fetch_reservoir_data():
    # Get query parameters from the frontend with default dates if not provided.
    start_date = request.args.get('start_date', '2020-01-01')
    end_date = request.args.get('end_date', '2025-01-01')

    results = {}

    # Ensure the DB table exists.
    create_table_if_not_exists()

    for name, station_id in reservoirs.items():
        params = {
            "Stations": station_id,
            "SensorNums": "6",
            "dur_code": "D",
            "Start": start_date,
            "End": end_date
        }

        try:
            response = requests.get(BASE_URL, params=params, timeout=10)
            response.raise_for_status()

            # Create filename using reservoir id.
            filename = os.path.join(CSV_FOLDER, f"{station_id}.csv")
            # If file exists, remove it before storing new data.
            if os.path.exists(filename):
                os.remove(filename)

            # Write the CSV data to file.
            with open(filename, "w") as f:
                f.write(response.text)

            # Filter and process the CSV data.
            filtered_data = filter_csv_data(filename)
            if filtered_data:
                db_result = store_data_in_db(filtered_data)
            else:
                db_result = "No valid data to insert"

            results[name] = {
                "downloaded": f"{filename} ({len(response.text.splitlines())} lines)",
                "db_insertion": db_result
            }
        except Exception as e:
            results[name] = {"error": str(e)}

    return jsonify({
        "status": "done",
        "start_date": start_date,
        "end_date": end_date,
        "results": results
    })


@app.route('/')
def index():
    return "Flask API is running"


if __name__ == '__main__':
    app.run(debug=True)
