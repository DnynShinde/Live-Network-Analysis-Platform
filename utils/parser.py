# Converts raw SDN JSON responses into flat dictionaries for Kafka

def parse_switches(switches_json):
    """Parse switch data according to ontology requirements."""
    result = []
    for switch in switches_json:
        # Extract required and any available optional properties
        switch_data = {'dpid': switch['dpid']}
        
        # Add optional properties if available
        if 'name' in switch:
            switch_data['name'] = switch['name']
        if 'model' in switch:
            switch_data['model'] = switch['model']
        # Add more optional properties as needed
            
        result.append(switch_data)
    return result

def parse_hosts(response_json):
    """Parse host data according to ontology requirements."""
    print("DEBUG: Hosts API response:", response_json)
    if not isinstance(response_json, list):
        return []
    
    result = []
    for host in response_json:
        if isinstance(host, dict) and 'mac' in host:
            # Required property
            host_data = {'mac': host['mac']}
            
            # Optional properties
            if 'ipv4' in host and host['ipv4']:
                host_data['ip'] = host['ipv4'][0]
            if 'hostname' in host:
                host_data['hostname'] = host['hostname']
                
            # Relationship data
            if 'port' in host and isinstance(host['port'], dict):
                if 'dpid' in host['port']:
                    host_data['attached_switch'] = host['port']['dpid']
                if 'port_no' in host['port']:
                    host_data['port_no'] = host['port']['port_no']
                    
            result.append(host_data)
    return result

def parse_links(links_json):
    """Parse link data according to ontology requirements."""
    result = []
    for link in links_json:
        # Extract required properties for the relationship
        link_data = {
            'src_dpid': link['src']['dpid'],
            'src_port': link['src']['port_no'],
            'dst_dpid': link['dst']['dpid'],
            'dst_port': link['dst']['port_no']
        }
        
        # Add optional properties if available
        if 'bandwidth' in link:
            link_data['bandwidth'] = link['bandwidth']
        if 'latency' in link:
            link_data['latency'] = link['latency']
            
        result.append(link_data)
    return result
