from neo4j_app.graph_manager import GraphManager

# Create a singleton instance of GraphManager
graph_manager = GraphManager()

def handle_switch(session, data):
    """Handle switch data using the ontology-based graph manager."""
    try:
        graph_manager.create_node('Switch', data)
        print(f"Successfully processed Switch: {data}")
    except Exception as e:
        print(f"Error processing Switch {data}: {e}")

def handle_host(session, data):
    """Handle host data using the ontology-based graph manager."""
    try:
        graph_manager.create_node('Host', data)
        print(f"Successfully processed Host: {data}")
        
        # If we have attachment information, create relationship to switch
        if 'attached_switch' in data:
            switch_data = {'dpid': data['attached_switch']}
            rel_data = {'port_no': data.get('port_no')}
            graph_manager.create_relationship('CONNECTED_TO', data, switch_data, rel_data)
            print(f"Created CONNECTED_TO relationship for Host {data['mac']}")
    except Exception as e:
        print(f"Error processing Host {data}: {e}")

def handle_link(session, data):
    """Handle link data using the ontology-based graph manager."""
    try:
        src_switch = {'dpid': data['src_dpid']}
        dst_switch = {'dpid': data['dst_dpid']}
        rel_data = {
            'src_port': data['src_port'],
            'dst_port': data['dst_port']
        }
        
        graph_manager.create_relationship('LINKS_TO', src_switch, dst_switch, rel_data)
        print(f"Successfully processed Link: {data}")
    except Exception as e:
        print(f"Error processing Link {data}: {e}")
