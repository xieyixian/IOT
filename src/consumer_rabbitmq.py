import pika
import json

def callback(ch, method, properties, body):
    try:
        # 解析收到的JSON消息
        data = json.loads(body.decode('utf-8'))

        # 筛选出Value大于50的数据
        if data.get('Value') is not None and data['Value'] < 50:
            # 保存数据，这里你可以根据需求选择存储的方式
            save_data(data)

    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")

def save_data(data):
    # 在这里添加保存数据的逻辑，可以存储到文件、数据库等
    print(f"Received and saved data: {data}")

def receive_data_from_rabbitmq():
    rabbitmq_host = "192.168.0.100"
    rabbitmq_port = 5673
    rabbitmq_user = "xyh123"
    rabbitmq_password = "123456"
    rabbitmq_queue = "CSC8112"

    # 建立到RabbitMQ的连接
    credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)
    parameters = pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port, credentials=credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    # 声明队列
    channel.queue_declare(queue=rabbitmq_queue)

    # 设置回调函数，处理接收到的消息
    channel.basic_consume(queue=rabbitmq_queue, on_message_callback=callback, auto_ack=True)

    print("Waiting for messages. To exit press CTRL+C")
    # 开始监听消息
    channel.start_consuming()

if __name__ == '__main__':
    receive_data_from_rabbitmq()
