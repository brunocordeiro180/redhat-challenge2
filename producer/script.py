import json
from kafka import KafkaProducer

def read_json_file(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data

def send_to_kafka(topic, bootstrap_servers, messages):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    for message in messages:
        producer.send(topic, message)
        print(f"Sent message: {message}")

    producer.flush()
    producer.close()

if __name__ == "__main__":
    file_path = 'movies-2020s.json'
    kafka_topic = 'movies'
    kafka_bootstrap_servers = ['localhost:29092'] 

    json_messages = read_json_file(file_path)
    send_to_kafka(kafka_topic, kafka_bootstrap_servers, json_messages)
