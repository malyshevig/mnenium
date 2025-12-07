import pika

class RabbitMQ:

    def __init__(self):
        self.connection = None
        self.init_connection()

    def init_connection (self):
        credentials = pika.PlainCredentials('bot', 'begemot')
        connection_params = pika.ConnectionParameters(
            host='localhost',  # Или IP-адрес сервера
            port=5672,
            credentials=credentials,

            virtual_host='/'  # Виртуальный хост по умолчанию

        )

        self.connection = pika.BlockingConnection(connection_params)



    def create_queue(self, queue_name):
        channel = self.connection.channel()

        # Создание очереди (если не существует)
        try:
            channel.queue_declare(queue=queue_name, durable=True)
        finally:
            if channel and channel.is_open:
                channel.close()

    def delete_queue(self, queue_name):
        channel = self.connection.channel()
        try:
            channel.queue_delete(queue=queue_name)
        finally:
            if channel and channel.is_open:
                channel.close()


    def publish_message(self, queue_name, message):
        if not self.connection.is_open:
           self.init_connection()

        channel = self.connection.channel()
        if channel.is_open:
            try:
                channel.basic_publish(
                    exchange='',
                    routing_key=queue_name,
                    body=message,
                    properties=pika.BasicProperties(
                            delivery_mode=2  # Делаем сообщение дurable
                    )
                )
            finally:
                if channel and channel.is_open:
                    channel.close()

    def listen(self, queues:dict):
        channel = self.connection.channel()
        for k, v in queues.items():
            channel.basic_consume(
                queue=k,
                on_message_callback=v,
                auto_ack=False  # Автоматическое подтверждение получения
            )
        channel.start_consuming()

    def close_connection(self):
        self.connection.close()


