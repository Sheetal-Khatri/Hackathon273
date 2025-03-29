import paho.mqtt.client as mqtt
import mysql.connector
import json

# Connect to MySQL database
db = mysql.connector.connect(
    host="reservoir-db.c1sk2imgwsr1.us-west-1.rds.amazonaws.com",
    user="admin",
    password="hackathoncmpe273",
    database="reservoir"
)
cursor = db.cursor()

# List of topics to subscribe to.
topics = [
    "station-ORO",  # Lake Oroville
    "station-SHA",  # Shasta Lake
    "station-CLE",  # Trinity Lake
    "station-NML",  # New Melones Lake
    "station-SNL",  # San Luis Reservoir
    "station-DNP",  # Don Pedro Reservoir
    "station-BER",  # Lake Berryessa
    "station-FOL",  # Folsom Lake
    "station-BUL",  # New Bullards Bar
    "station-PNF"   # Pine Flat Lake
]

def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT Broker with code:", rc)
    for topic in topics:
        client.subscribe(topic)
        print(f"Subscribed to topic: {topic}")

def on_message(client, userdata, msg):
    try:
        print(msg)
        payload = msg.payload.decode("utf-8")
        data = json.loads(payload)
        
        # Extract date and feet from the message
        date_val = data.get("DATE")   # Expected format "YYYY-MM-DD"
        feet_val = data.get("FEET")
        
        # Extract cdec_id from the topic, e.g. "reservoir-oro" => "ORO"
        topic_parts = msg.topic.split("-")
        cdec_id = topic_parts[1].upper() if len(topic_parts) >= 2 else None
        
        # Insert into filtered_data table with columns: date, cdec_id, feet.
        sql = """
            INSERT INTO filtered_data (date, cdec_id, feet)
            VALUES (%s, %s, %s)
        """
        values = (date_val, cdec_id, feet_val)
        cursor.execute(sql, values)
        db.commit()
        print(f"✅ Inserted into filtered_data: {values}")
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