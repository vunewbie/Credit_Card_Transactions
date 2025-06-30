from kafka import KafkaProducer
import os, csv, time, random, sys
from dotenv import load_dotenv, find_dotenv

# Load environment variables from a .env file
load_dotenv(find_dotenv())

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'default_topic')
DATA_FILE_PATH = os.getenv('DATA_FILE_PATH')
OFFSET_FILE_PATH = os.getenv('OFFSET_FILE_PATH')

def create_kafka_producer(bootstrap_servers):
    """
    Create and return a Kafka producer instance.

    Args:
        bootstrap_servers (str): The Kafka bootstrap servers to connect to.

    Returns:
        KafkaProducer: An instance of KafkaProducer connected to the specified servers.

    Raises:
        Exception: If the producer cannot be created or connected.
    """
    try:
        producer =  KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: str(v).encode('utf-8'),
        )
        print(f"Kafka producer created and connected to {bootstrap_servers}")
        return producer
    except Exception as e:
        raise Exception(f"Failed to create Kafka producer: {e}")
    

def read_last_offset(file_path):
    """
    Read the last offset from a file.
    
    Args:
        file_path (str): The path to the file containing the last offset.

    Returns:
        int: The last offset read from the file  
        
    Raises:
        FileNotFoundError: If the specified file does not exist.
        Exception: If there is an error reading the file.
    """
    
    
    try:
        with open(file_path, "r") as f:
            content = f.read().strip()
            if not content:
                return 0
            return int(content)
    except FileNotFoundError:
        raise FileNotFoundError(f"The file {file_path} does not exist.")
    except:
        raise Exception(f"Failed to read last offset from file {file_path}. Ensure the file contains a valid integer.")

def write_offset(file_path, offset):
    """
    Write the last offset to a file.

    Args:
        file_path (str): The path to the file where the offset will be written.
        offset (int): The offset value to write to the file.

    Raises:
        FileNotFoundError: If the specified file cannot be created or opened.
        Exception: If there is an error writing to the file.
    """
    try:
        with open(file_path, "w") as f:
            f.write(str(offset))
    except FileNotFoundError:
        raise FileNotFoundError(f"The file {file_path} cannot be created or opened.")
    except Exception as e:
        raise Exception(f"Failed to write offset to file {file_path}: {e}")


def read_transactions_from_csv(file_path, offset=0):
    """
    Read transactions from a CSV file and return a list of dictionaries.

    Args:
        file_path (str): The path to the CSV file containing transactions.
        offset (int): The number of rows to skip from the beginning of the file.

    Returns:
        list: A list of dictionaries representing transactions.

    Raises:
        FileNotFoundError: If the specified file does not exist.
        Exception: If there is an error reading the file.
    """

    try:
        with open(file_path, "r") as file:
            reader = csv.DictReader(file)
            for i, row in enumerate(reader):
                if i < offset:
                    continue
                yield i, row
    except FileNotFoundError:
        raise FileNotFoundError(f"The file {file_path} does not exist.")
    except Exception as e:
        raise Exception(f"Failed to read transactions from CSV file: {e}")
    
def send_transactions_to_kafka(producer, topic, transactions, start_offset):
    """
    Send a list of transactions to a Kafka topic.

    Args:
        producer (KafkaProducer): The Kafka producer instance.
        topic (str): The Kafka topic to send messages to.
        transactions (list): A list of transactions to send.
        start_offset (int): The starting offset for the transactions.

    Raises:
        Exception: If there is an error sending a transaction.
    """
    last_offset = start_offset
    try:
        for offset, transaction in transactions:
            try:
                producer.send(topic, value=transaction)
                print(f"[{transaction['Time']}-{transaction['Day']}:{transaction['Month']}:{transaction['Year']}] Sent: User {transaction['User']} - Amount {transaction['Amount']}, {transaction['Merchant City']} - {transaction['Merchant State']}")
                last_offset = offset + 1
                time.sleep(random.uniform(1, 5))
                
            except Exception as e:
                print(f"Failed to send transaction: {e}")
    except KeyboardInterrupt:
        print("\nSTOPPED BY USER (CTRL+C)")
    finally:
        write_offset(OFFSET_FILE_PATH, last_offset)
        print(f"Offset saved: {last_offset}")

def stop_kafka_producer(producer):
    """
    Stop the Kafka producer gracefully.

    Args:
        producer (KafkaProducer): The Kafka producer instance to stop.

    Raises:
        Exception: If there is an error stopping the producer.
    """
    print("Stopping Kafka producer...")
    try:
        producer.flush()
        producer.close()
        print("Kafka producer stopped successfully.")
    except Exception as e:
        print(f"Failed to stop Kafka producer: {e}")
    finally:
        sys.exit(0)

def main():
    """
    Main function to create a Kafka producer, read transactions from a CSV file,
    and send them to a Kafka topic.
    """
    producer = None
    try:
        # Load environment variables
        print("Loading environment variables...")
        if not KAFKA_BOOTSTRAP_SERVERS or not KAFKA_TOPIC or not DATA_FILE_PATH or not OFFSET_FILE_PATH:
            raise ValueError("Environment variables KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, OFFSET_FILE_PATH, and DATA_FILE_PATH must be set.")
        
        offset = read_last_offset(OFFSET_FILE_PATH)
        if offset < 0:
            raise ValueError("Offset must be a non-negative integer.")
        else:
            print(f"Resuming from line: {offset}")


        print("Starting Kafka producer...")

        producer = create_kafka_producer(KAFKA_BOOTSTRAP_SERVERS)

        print(f"Reading transactions from CSV file '{DATA_FILE_PATH}'...")
        transactions = read_transactions_from_csv(DATA_FILE_PATH, offset)
        
        if transactions:
            print(f"Sending transactions to Kafka topic '{KAFKA_TOPIC}'...")
            send_transactions_to_kafka(producer, KAFKA_TOPIC, transactions, offset)
        else:
            print("No transactions to send.")
    
    except ValueError as ve:
        print(f"Configuration error: {ve}")

    except FileNotFoundError as e:
        print(f"File error: {e}")

    except Exception as e:
        print(f"An error occurred: {e}")
    
    finally:
        if producer:
            stop_kafka_producer(producer)
        else:
            print("Producer was not initialized. Exiting...")
            sys.exit(1)

if __name__ == "__main__":
    main()



    

    

