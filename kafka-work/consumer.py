
from confluent_kafka import Consumer, KafkaException
import sys

def message_handler(consumer, msg):
    print(f"Consumed message: {msg.value().decode('utf-8')} from topic: {msg.topic()}")

def main():
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'my-group',
        'auto.offset.reset': 'earliest'
    }

    topics = ['topic1', 'topic2', 'topic3']  # Replace with your actual topics

    consumer = Consumer(conf)
    try:
        consumer.subscribe(topics)

        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    sys.stderr.write(f"Reached end of partition {msg.topic()} [{msg.partition()}] offset {msg.offset()}\n")
                else:
                    sys.stderr.write(f"Error: {msg.error()}\n")
            else:
                message_handler(consumer, msg)

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == '__main__':
    main()






