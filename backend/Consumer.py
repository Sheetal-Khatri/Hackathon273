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

        # Parse fields for filtered_data
        obs_date_raw = data.get("OBS_DATE", "")  # Ex: "20220102 0000"
        if not obs_date_raw:
            raise ValueError("Missing OBS_DATE in payload")

        # Extract date part and convert to 'YYYY-MM-DD'
        obs_date = obs_date_raw[:8]  # "20220102"
        date = f"{obs_date[:4]}-{obs_date[4:6]}-{obs_date[6:]}"  # → "2022-01-02"

        cdec_id = data["STATION_ID"]
        feet = float(data["VALUE"])

        sql = """
            INSERT INTO filtered_data (date, cdec_id, feet)
            VALUES (%s, %s, %s)
        """
        values = (date, cdec_id, feet)
        cursor.execute(sql, values)
        db.commit()

        print("✅ Inserted into filtered_data")

    except Exception as e:
        print("Error inserting into filtered_data:", e)

def main():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect("localhost", 1883)
    print("MQTT Consumer started. Waiting for messages...")
    client.loop_forever()

if __name__ == "__main__":
    main()