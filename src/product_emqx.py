import json
from paho.mqtt import client as mqtt_client
import pandas as pd

def send_data_emqx(data):
    mqtt_ip = "192.168.0.102"
    mqtt_port = 1883
    topic = "CSC8112"

    # Create a MQTT client object
    client = mqtt_client.Client()

    # Callback function for MQTT connection
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT OK!")
        else:
            print(f"Failed to connect, return code {rc}")

    # Connect to MQTT service
    client.on_connect = on_connect
    client.connect(mqtt_ip, mqtt_port)

    try:
        # Convert Pandas DataFrame to JSON array
        data_json = data.to_json(orient='records')

        # Publish message to MQTT as a single JSON array
        client.publish(topic, data_json)
        print(f"Data published to MQTT: {data_json}")

    except Exception as e:
        print(f"Error publishing data to MQTT: {e}")

    finally:
        # Disconnect from MQTT service
        client.disconnect()

if __name__ == '__main__':
    # Example DataFrame
    df = pd.DataFrame({
        "Timestamp": [1673913780000.0, 1673914020000.0, 1673914260000.0],
        "Value": [8.0, 5.0, 5.0]
    })

    # Run the send_data_emqx function
    send_data_emqx(df)
