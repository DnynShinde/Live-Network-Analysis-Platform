import time
import requests
from kafka import KafkaProducer
import json
from utils.config_loader import load_config
from utils.parser import parse_switches, parse_hosts, parse_links

config = load_config()

producer = KafkaProducer(
    bootstrap_servers=config['kafka']['bootstrap_servers'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_and_publish(endpoint, parser_func, topic):
    url = config['sdn_controller']['base_url'] + endpoint
    try:
        response = requests.get(url)
        response.raise_for_status()
        parsed_data = parser_func(response.json())
        for item in parsed_data:
            producer.send(topic, item)
            print(f"Published to {topic}: {item}")
    except Exception as e:
        print(f"Error fetching {url}: {e}")

# Polling function to fetch data from SDN controller and publish to Kafka
def run_poller():
    while True:
        # Fetch and publish data for switches
        fetch_and_publish(config['sdn_controller']['endpoints']['switches'],
                          parse_switches, config['kafka']['topics']['switches'])
        # Fetch and publish data for hosts and hosts
        fetch_and_publish(config['sdn_controller']['endpoints']['hosts'],
                          parse_hosts, config['kafka']['topics']['hosts'])
        # Fetch and publish data for links
        fetch_and_publish(config['sdn_controller']['endpoints']['links'],
                          parse_links, config['kafka']['topics']['links'])

        time.sleep(config['sdn_controller']['poll_interval_seconds'])

if __name__ == "__main__":
    run_poller()
