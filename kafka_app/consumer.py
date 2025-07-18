from kafka import KafkaConsumer
import json
from neo4j import GraphDatabase
from utils.config_loader import load_config
from neo4j_app.updater import handle_switch, handle_host, handle_link

config = load_config()

consumer = KafkaConsumer(
    config['kafka']['topics']['switches'],
    config['kafka']['topics']['hosts'],
    config['kafka']['topics']['links'],
    bootstrap_servers=config['kafka']['bootstrap_servers'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

neo4j_driver = GraphDatabase.driver(
    config['neo4j']['uri'],
    auth=(config['neo4j']['user'], config['neo4j']['password'])
)

def handle_switch(session, data):
    print(f"Running Cypher for switch: {data}")
    session.run("MERGE (s:Switch {dpid: $dpid})", dpid=data['dpid'])

def handle_host(session, data):
    print("DEBUG handle_host data:", data)
    # ...existing code...

def parse_hosts(response_json):
    print("DEBUG: Hosts API response:", response_json)
    if not isinstance(response_json, list):
        return []
    result = []
    for host in response_json:
        if isinstance(host, dict):
            mac = host.get('mac')
            ip = host.get('ipv4', [None])[0] if 'ipv4' in host and host['ipv4'] else None
            attached_switch = host.get('port', {}).get('dpid')
            if mac and ip and attached_switch:
                result.append({'mac': mac, 'ip': ip, 'attached_switch': attached_switch})
    return result

def run_consumer():
    with neo4j_driver.session(database=config['neo4j']['database']) as session:
        for message in consumer:
            topic = message.topic
            data = message.value
            if topic == config['kafka']['topics']['switches']:
                handle_switch(session, data)
            elif topic == config['kafka']['topics']['hosts']:
                handle_host(session, data)
            elif topic == config['kafka']['topics']['links']:
                handle_link(session, data)

if __name__ == "__main__":
    run_consumer()
