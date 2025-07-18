def handle_switch(session, data):
    session.run("""
        MERGE (s:Switch {dpid: $dpid})
    """, dpid=data['dpid'])

def handle_host(session, data):
    session.run("""
        MERGE (h:Host {mac: $mac, ip: $ip})
        MERGE (s:Switch {dpid: $dpid})
        MERGE (h)-[:CONNECTED_TO]->(s)
    """, mac=data['mac'], ip=data['ip'], dpid=data['attached_switch'])

def handle_link(session, data):
    session.run("""
        MERGE (s1:Switch {dpid: $src})
        MERGE (s2:Switch {dpid: $dst})
        MERGE (s1)-[:CONNECTED_TO {src_port: $src_port, dst_port: $dst_port}]->(s2)
    """, src=data['src_dpid'], dst=data['dst_dpid'],
         src_port=data['src_port'], dst_port=data['dst_port'])
