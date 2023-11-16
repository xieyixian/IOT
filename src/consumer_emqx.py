import json
from paho.mqtt import client as mqtt_client

def consumer_data_emqx():
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

    data_list = []
    def on_message(client, userdata, msg):
        try:
            # Directly load JSON array from the payload
            payload = json.loads(msg.payload)
            for record in payload:
                print(f"Get message from publisher {record}")

                # If you want to store the data in a list for further processing:
                data_list.append(record)
            print(f"Get message from publisher {payload}")
            print(f"Get message from publi11111sher {data_list}")
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")

    # Subscribe MQTT topic
    client.subscribe(topic)
    client.on_message = on_message

    # Start a thread to monitor messages from the publisher
    client.loop_forever()

if __name__ == '__main__':
    # Run the consumer_data_emqx function
    consumer_data_emqx()
