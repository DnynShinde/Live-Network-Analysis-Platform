# Converts raw SDN JSON responses into flat dictionaries for Kafka

def parse_switches(switches_json):
    return [{'dpid': s['dpid']} for s in switches_json]

def parse_hosts(response_json):
    print("DEBUG: Hosts API response:", response_json)
    if not isinstance(response_json, list):
        return []
    result = []
    for host in response_json:
        if isinstance(host, dict) and 'mac' in host:
            result.append({'mac': host['mac']})
    return result

def parse_links(links_json):
    return [{
        'src_dpid': l['src']['dpid'],
        'src_port': l['src']['port_no'],
        'dst_dpid': l['dst']['dpid'],
        'dst_port': l['dst']['port_no']
    } for l in links_json]
