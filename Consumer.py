import paho.mqtt.client as mqtt
import json

def on_connect(client, userdata, flags, rc):
    print("Connected with result code:", rc)
    # Subscribe to all topics under "station"
    client.subscribe("#")

def on_message(client, userdata, msg):
    if msg.topic.startswith("station-"):
        try:
            payload = msg.payload.decode("utf-8")
            data = json.loads(payload)
            print(f"Received message from topic '{msg.topic}': {data}")
        except Exception as e:
            print(f"Error processing message from topic '{msg.topic}': {e}")

def main():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect("localhost", 1883, 60)
    print("MQTT Consumer started. Waiting for messages...")
    client.loop_forever()

if __name__ == "__main__":
    main()