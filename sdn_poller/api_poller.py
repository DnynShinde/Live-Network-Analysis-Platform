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

def run_poller():
    while True:
        fetch_and_publish(config['sdn_controller']['endpoints']['switches'],
                          parse_switches, config['kafka']['topics']['switches'])
        fetch_and_publish(config['sdn_controller']['endpoints']['hosts'],
                          parse_hosts, config['kafka']['topics']['hosts'])
        fetch_and_publish(config['sdn_controller']['endpoints']['links'],
                          parse_links, config['kafka']['topics']['links'])

        time.sleep(config['sdn_controller']['poll_interval_seconds'])

def handle_host(session, data):
    print("DEBUG handle_host data:", data)
    mac = data.get('mac')
    ip = data.get('ip') or "unknown"
    dpid = data.get('attached_switch')
    session.run(
        "MERGE (h:Host {mac: $mac, ip: $ip, attached_switch: $dpid})",
        mac=mac, ip=ip, dpid=dpid
    )

def parse_hosts(response_json):
    print("DEBUG: Hosts API response:", response_json)
    if not isinstance(response_json, list):
        return []
    result = []
    for host in response_json:
        if isinstance(host, dict):
            mac = host.get('mac')
            ip = host.get('ipv4', [None])[0] if 'ipv4' in host and host['ipv4'] else None
            attached_switch = host.get('attached_switch', host.get('port', {}).get('dpid'))
            # Allow hosts even if ip is None
            if mac and attached_switch:
                result.append({'mac': mac, 'ip': ip, 'attached_switch': attached_switch})
    return result

if __name__ == "__main__":
    run_poller()
