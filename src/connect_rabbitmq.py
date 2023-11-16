import pandas as pd
import pika
import json
from paho.mqtt import client as mqtt_client
from datetime import datetime, timedelta

daily_data = {}  # Dictionary to store daily data and counts
average_value = {} # Dictionary to store daily data and value
average_data = {}  # Dictionary to store average data

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT OK!")
    else:
        print("Failed to connect, return code %d\n", rc)

def on_message(client, userdata, msg):
    try:
        # Assuming the payload is in JSON format
        payload = json.loads(msg.payload)
        # Check if the 'Value' is greater than 50

        process_data(payload)


    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        print(f"Problematic payload: {payload}")
    except Exception as e:
        print(f"Error processing message: {e}")


def process_data(data):
    j=0
    try:
        global average_data
        average_data = {}
        global daily_data
        daily_data = {}
        global average_value
        average_value = {}

        for i in range(len(data)):
            # Extract the date from the timestamp
            if data[i]['Value'] < 50:
                print(data[i])
                timestamp = data[i].get('Timestamp', '')
                date_str = datetime.utcfromtimestamp(timestamp/1000).strftime('%Y-%m-%d')
                if date_str not in daily_data:
                    daily_data[date_str] = {'sum': 0, 'count': 0}
                    j+=1

                # Accumulate values for the day
                daily_data[date_str]['sum'] += data[i]['Value']
                daily_data[date_str]['count'] += 1

                # Calculate the average
                average_value[date_str] = daily_data[date_str]['sum'] / daily_data[date_str]['count']
                # Create a new data object with the average value
                average_data[j-1] = {
                    'Timestamp': convert_timestamp_to_datetime(timestamp),
                    'Value': average_value[date_str]
                }
            else:
                print(f"Value is greater than 50{data[i]}")




        # data_new = pd.DataFrame()
        # for i in range(len(average_data)):
        #     data_new.loc[i, 'Timestamp'] = average_data[i].get('Timestamp', '')
        #     data_new.loc[i, 'Value'] = average_data[i].get('Value', '')
        data_df = pd.DataFrame.from_dict(average_data, orient='index')
        data_df['Timestamp'] = pd.to_datetime(data_df['Timestamp'], format='%Y-%m-%d %H:%M:%S')
        data_df = data_df.sort_values(by='Timestamp').reset_index(drop=True)
        data_df['Timestamp'] = data_df['Timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

        print(data_df)
        publish_to_rabbitmq(average_data)

    except Exception as e:
        print(f"Error processing data:111 {e}")

def convert_timestamp_to_datetime(timestamp):
    try:
        # Assuming the timestamp is in seconds (adjust if it's in milliseconds or microseconds)
        timestamp_in_seconds = int(timestamp)

        # Convert the timestamp to a datetime object
        dt_object = pd.to_datetime(timestamp_in_seconds, unit='ms')

        date_obj = datetime.utcfromtimestamp(timestamp / 1000)
        start_of_day = date_obj.replace(hour=0, minute=0, second=0, microsecond=0)
        formatted_start_of_day = start_of_day.strftime("%Y-%m-%d %H:%M:%S")
        # Format the datetime object as a string
        # formatted_datetime = dt_object.strftime("%Y-%m-%d %H:%M:%S")

        return formatted_start_of_day
    except Exception as e:
        print(f"Error converting timestamp to datetime: {e}")
        return None

def publish_to_rabbitmq(data):
    rabbitmq_host = "192.168.0.100"
    rabbitmq_port = 5673
    rabbitmq_user = "xyh123"
    rabbitmq_password = "123456"
    rabbitmq_queue = "CSC8112"

    # Connect to RabbitMQ
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=rabbitmq_host,
            port=rabbitmq_port,
            credentials=pika.PlainCredentials(rabbitmq_user, rabbitmq_password)
        )
    )
    channel = connection.channel()

    # Declare a queue
    channel.queue_declare(queue=rabbitmq_queue)

    # Publish the data to RabbitMQ
    channel.basic_publish(exchange='',
                          routing_key=rabbitmq_queue,
                          body=json.dumps(data))

    print(f"Data published to RabbitMQ: {data}")

    # Close the connection
    connection.close()

def consume_from_emqx():
    # The EMQX consuming logic remains unchanged
    emqx_host = "192.168.0.102"
    emqx_port = 1883
    emqx_topic = "CSC8112"

    client = mqtt_client.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(emqx_host, emqx_port)
    client.subscribe(emqx_topic)

    client.loop_forever()

if __name__ == '__main__':
    consume_from_emqx()
