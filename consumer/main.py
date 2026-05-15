import json
import os
import time
import pika

RABBITMQ_URL = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")

def callback(ch, method, properties, body):
    data = json.loads(body)
    print(f"EVENT: {data['event']} | node: {data['node_name']} | time: {data['timestamp']}", flush=True)
    ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    while True:
        try:
            params = pika.URLParameters(RABBITMQ_URL)
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            channel.queue_declare(queue="node_events", durable=True)
            channel.basic_consume(queue="node_events", on_message_callback=callback)
            print("Consumer ready, waiting for events...", flush=True)
            channel.start_consuming()
        except Exception as e:
            print(f"Connection failed: {e}. Retrying in 5s...", flush=True)
            time.sleep(5)

if __name__ == "__main__":
    main()
