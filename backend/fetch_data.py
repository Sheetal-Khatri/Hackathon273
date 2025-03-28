from flask import Flask, request, jsonify
import requests
import os
from datetime import date

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


@app.route('/api/fetch-data', methods=['GET'])
def fetch_reservoir_data():
    # Get query parameters from frontend
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date', str(date.today()))

    if not start_date:
        return jsonify({"error": "start_date is required"}), 400

    results = {}

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
            filename = os.path.join(CSV_FOLDER, f"{name}.csv")

            with open(filename, "w") as f:
                f.write(response.text)

            results[name] = f"Downloaded: {filename} ({len(response.text.splitlines())} lines)"
        except Exception as e:
            results[name] = f"Failed: {str(e)}"

    return jsonify({"status": "done", "start_date": start_date, "end_date": end_date, "results": results})


# Optional: root route to verify server is running
@app.route('/')
def index():
    return "Flask API is running"

