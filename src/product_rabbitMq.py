import pika

def send_message_to_rabbitmq(message):
    rabbitmq_host = "192.168.0.100"
    rabbitmq_port = 5673  # Default RabbitMQ port
    rabbitmq_user = "xyh123"
    rabbitmq_password = "123456"

    # Establish a connection to RabbitMQ
    credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port, credentials=credentials))
    channel = connection.channel()

    # Declare a queue (create if not exists)
    channel.queue_declare(queue='hello_queue')

    # Publish the message to the 'hello_queue'
    channel.basic_publish(exchange='', routing_key='hello_queue', body=message)

    print(f" [x] Sent '{message}' to RabbitMQ")

    # Close the connection
    connection.close()

if __name__ == '__main__':
    message_to_send = "Hello!"
    send_message_to_rabbitmq(message_to_send)
