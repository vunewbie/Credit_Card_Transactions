from confluent_kafka import Producer
from dotenv import load_dotenv
import csv, time, random, sys, signal, json, os

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_CLIENT_ID = os.getenv("KAFKA_CLIENT_ID")

def create_producer():
    config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": KAFKA_CLIENT_ID,
    }

    return Producer(config)

def read_transactions(file_path):
    try:
        with open(file_path, "r") as file:
            reader = csv.DictReader(file)
            
            for row in reader:
                yield row # return current row and retain the generator state
    except Exception as e:
        print(f"Lỗi đọc file: {e}")
        sys.exit(1)

def send_messages(producer, topic, transactions):
    for row in transactions:
        producer.produce(topic, value=json.dumps(row).encode('utf-8'))
        print(f"Đã gửi: {row}")
        producer.poll(0)
        time.sleep(random.uniform(1, 5))

def shutdown_producer(producer):
    print("Đang đóng producer...")
    producer.flush()
    print("Producer đã đóng.")
    sys.exit(0)

def main():
    file_path = "data/credit_card_transactions.csv"
    producer = create_producer()

    signal.signal(signal.SIGINT, lambda s, f: shutdown_producer(producer))

    transactions = read_transactions(file_path)
    try:
        send_messages(producer, KAFKA_TOPIC, transactions)
    except Exception as e:
        print(f"Lỗi khi gửi dữ liệu: {e}")
    finally:
        shutdown_producer(producer)

if __name__ == '__main__':
    main()
