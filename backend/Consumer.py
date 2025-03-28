import paho.mqtt.client as mqtt
import mysql.connector
import json

db = mysql.connector.connect(
    host="reservoir-db.c1sk2imgwsr1.us-west-1.rds.amazonaws.com",
    user="admin",
    password="hackathoncmpe273",
    database="reservoir"
)
cursor = db.cursor()

topics = [
    "reservoir/oro",  # Lake Oroville
    "reservoir/sha",  # Shasta Lake
    "reservoir/cle",  # Trinity Lake
    "reservoir/nml",  # New Melones Lake
    "reservoir/snl",  # San Luis Reservoir
    "reservoir/dnp",  # Don Pedro Reservoir
    "reservoir/ber",  # Lake Berryessa
    "reservoir/fol",  # Folsom Lake
    "reservoir/bul",  # New Bullards Bar
    "reservoir/pnf"   # Pine Flat Lake
]

# def on_connect(client, userdata, flags, rc):
#     print("Connected with result code:", rc)
#     # Subscribe to all topics under "station"
#     client.subscribe("#")

def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT Broker with code:", rc)
    for topic in topics:
        client.subscribe(topic)
        print(f"Subscribed to topic: {topic}")

def on_message(client, userdata, msg):
    if msg.topic.startswith("station-"):
        try:
            payload = msg.payload.decode("utf-8")
            data = json.loads(payload)
            print(f"Received message from topic '{msg.topic}': {data}")
        except Exception as e:
            print(f"Error processing message from topic '{msg.topic}': {e}")

def on_message(client, userdata, msg):
    print(f"\n📥 Message from {msg.topic}: {msg.payload.decode()}")

    try:
        data = json.loads(msg.payload.decode())
        station_id = data["STATION_ID"]
        duration = data["DURATION"]
        sensor_number = data["SENSOR_NUMBER"]
        sensor_type = data["SENSOR_TYPE"]
        date_time = data["DATE_TIME"]
        obs_date = data["OBS_DATE"]
        value = data["VALUE"]
        data_flag = data.get("DATA_FLAG", "")
        units = data["UNITS"]

        sql = """
            INSERT INTO reservoir_data 
            (STATION_ID, DURATION, SENSOR_NUMBER, SENSOR_TYPE, DATE_TIME, OBS_DATE, VALUE, DATA_FLAG, UNITS)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        values = (station_id, duration, sensor_number, sensor_type, date_time, obs_date, value, data_flag, units)
        cursor.execute(sql, values)
        db.commit()

        print("✅ Inserted into DB")

    except Exception as e:
        print("Error processing message:", e)

def main():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect("localhost", 1883)
    print("MQTT Consumer started. Waiting for messages...")
    client.loop_forever()

if __name__ == "__main__":
    main()