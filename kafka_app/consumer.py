from kafka import KafkaConsumer
import json
from utils.config_loader import load_config
from neo4j_app.updater import handle_switch, handle_host, handle_link
from neo4j import GraphDatabase

# Load configuration
config = load_config()

# Initialize Kafka consumer
consumer = KafkaConsumer(
    config['kafka']['topics']['switches'],
    config['kafka']['topics']['hosts'],
    config['kafka']['topics']['links'],
    bootstrap_servers=config['kafka']['bootstrap_servers'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Initialize Neo4j driver
driver = GraphDatabase.driver(
    config['neo4j']['uri'],
    auth=(config['neo4j']['user'], config['neo4j']['password'])
)

def run_consumer():
    """Run the Kafka consumer and process messages."""
    print("Starting Kafka consumer...")
    for message in consumer:
        topic = message.topic
        data = message.value
        
        # No session required anymore as the handlers use graph_manager
        if topic == config['kafka']['topics']['switches']:
            handle_switch(None, data)
        elif topic == config['kafka']['topics']['hosts']:
            handle_host(None, data)
        elif topic == config['kafka']['topics']['links']:
            handle_link(None, data)

if __name__ == "__main__":
    run_consumer()
