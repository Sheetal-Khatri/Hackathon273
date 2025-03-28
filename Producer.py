from flask import Flask, request, jsonify
from flask_cors import CORS  # Import Flask-CORS
import json
import os
import time
import paho.mqtt.client as mqtt
import mysql.connector
from mysql.connector import Error

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Configuration file to persist reservoir configurations
CONFIG_FILE = "reservoir_configs.json"

# MySQL connection parameters – update these for your environment
MYSQL_CONFIG = {
    "host": "127.0.0.1",
    "user": "aditya",
    "password": "cm9vdA==",
    "database": "reservoir_database"
}

@app.route("/api/configs", methods=["POST"])
def update_configs():
    """
    Receives a JSON array of reservoir configurations and writes them to a config file.
    Each configuration should include reservoir details:
      - name
      - cdecId (used to construct topic names)
      - startDate
      - endDate
    """
    updated_config = request.json

    if not isinstance(updated_config, list):
        return jsonify({"error": "Request body must be a list of configs"}), 400

    # Write configuration to a file
    with open(CONFIG_FILE, "w") as f:
        json.dump(updated_config, f, indent=2)

    # Optionally, return the list of MQTT topics that would be created
    topics = []
    for reservoir in updated_config:
        cdec_id = reservoir.get("cdecId")
        if cdec_id:
            topic_name = f"station-{cdec_id}"
            topics.append(topic_name)

    return jsonify({
        "message": "Configuration updated successfully.",
        "topics": topics
    }), 200

@app.route("/api/run", methods=["GET"])
def run_configs():
    """
    Reads the saved reservoir configuration file, then for each reservoir:
      - Constructs an MQTT topic (e.g., station-SHA, station-ORO)
      - Connects to a MySQL database using MYSQL_CONFIG.
      - Fetches records from table 'reservoir_data' where:
            - 'cdec_id' matches the reservoir's cdecId, and
            - 'date' is between the configured startDate and endDate.
      - Publishes each record as a JSON message to the corresponding MQTT topic.
    """
    if not os.path.exists(CONFIG_FILE):
        return jsonify({"error": "No configuration found. Please configure reservoirs first."}), 400

    # Read configuration from file
    with open(CONFIG_FILE, "r") as f:
        configs = json.load(f)

    # Connect to the MySQL database
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        if conn.is_connected():
            print("Connected to MySQL database")
    except Error as e:
        return jsonify({"error": "Failed to connect to MySQL database", "details": str(e)}), 500

    cursor = conn.cursor()

    # Set up MQTT client (make sure your broker, e.g., Mosquitto, is running)
    mqtt_client = mqtt.Client()
    mqtt_client.connect("localhost", 1883, 60)
    mqtt_client.loop_start()  # start loop to process network events

    results = {}

    for reservoir in configs:
        cdec_id = reservoir.get("cdecId")
        start_date = reservoir.get("startDate")
        end_date = reservoir.get("endDate")
        topic_name = f"station-{cdec_id}"

        results[cdec_id] = {"topic": topic_name, "records_published": 0}

        # SQL query: Assumes a table 'reservoir_data' with columns: date, cdec_id, feet
        query = """
            SELECT date, feet FROM reservoir_data
            WHERE cdec_id = %s AND date BETWEEN %s AND %s
            ORDER BY date ASC
        """
        try:
            cursor.execute(query, (cdec_id, start_date, end_date))
            rows = cursor.fetchall()
        except Error as e:
            print(f"Error executing query for {cdec_id}: {e}")
            continue

        # Publish each record to the MQTT topic
        for row in rows:
            message = {
                "DATE": row[0].strftime("%Y-%m-%d") if hasattr(row[0], 'strftime') else row[0],
                "FEET": float(row[1])
            }
            mqtt_client.publish(topic_name, json.dumps(message))
            results[cdec_id]["records_published"] += 1
            # Optional: delay between messages if desired
            # time.sleep(0.5)

    # Clean up MQTT and MySQL connections
    mqtt_client.loop_stop()
    mqtt_client.disconnect()
    cursor.close()
    conn.close()

    return jsonify({
        "message": "Data fetched from MySQL and published to MQTT topics.",
        "details": results
    }), 200

if __name__ == "__main__":
    app.run(debug=True, port=5000)