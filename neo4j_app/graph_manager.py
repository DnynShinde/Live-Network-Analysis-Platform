from neo4j import GraphDatabase
from utils.config_loader import load_config
from utils.ontology_loader import get_node_properties, get_relationship_properties

class GraphManager:
    def __init__(self):
        config = load_config()
        self.driver = GraphDatabase.driver(
            config['neo4j']['uri'],
            auth=(config['neo4j']['user'], config['neo4j']['password'])
        )
        self.database = config['neo4j']['database']
    
    def close(self):
        self.driver.close()
    
    def create_node(self, node_type, data):
        """Dynamically create or update a node based on ontology."""
        properties = get_node_properties(node_type)
        
        # Check required properties
        for prop in properties['required']:
            if prop not in data:
                raise ValueError(f"Required property '{prop}' missing for {node_type}")
        
        # Build property dict with only defined properties
        props = {}
        for prop in properties['required'] + properties['optional']:
            if prop in data:
                props[prop] = data[prop]
        
        # Get primary key (first required property)
        primary_key = properties['required'][0]
        
        # Generate dynamic labels
        labels = [node_type] + properties['labels']
        label_string = ':'.join(labels)
        
        with self.driver.session(database=self.database) as session:
            # Build SET clause for optional properties
            set_clauses = []
            params = {primary_key: data[primary_key]}
            
            for prop, value in props.items():
                if prop != primary_key and value is not None:
                    set_clauses.append(f"n.{prop} = ${prop}")
                    params[prop] = value
            
            set_clause = " ".join(set_clauses)
            if set_clause:
                set_clause = "SET " + set_clause
            
            # Execute query
            query = f"""
            MERGE (n:{label_string} {{{primary_key}: ${primary_key}}})
            {set_clause}
            RETURN n
            """
            
            result = session.run(query, params)
            return result.single()
    
    def create_relationship(self, rel_type, source_data, target_data, rel_data=None):
        """Dynamically create a relationship based on ontology."""
        if rel_data is None:
            rel_data = {}
            
        rel_props = get_relationship_properties(rel_type)
        source_type = rel_props['source']
        target_type = rel_props['target']
        
        # Get primary keys
        source_primary_key = get_node_properties(source_type)['required'][0]
        target_primary_key = get_node_properties(target_type)['required'][0]
        
        # Check required properties
        for prop in rel_props['required']:
            if prop not in rel_data:
                raise ValueError(f"Required property '{prop}' missing for {rel_type} relationship")
        
        # Build property dict
        props = {}
        for prop in rel_props['required'] + rel_props['optional']:
            if prop in rel_data:
                props[prop] = rel_data[prop]
        
        # Build SET clause for properties
        set_clauses = []
        params = {
            "source_key": source_data[source_primary_key],
            "target_key": target_data[target_primary_key]
        }
        
        for prop, value in props.items():
            if value is not None:
                set_clauses.append(f"r.{prop} = ${prop}")
                params[prop] = value
        
        # Join with commas, not spaces
        set_clause = ", ".join(set_clauses)  # Changed from " ".join(set_clauses)
        if set_clause:
            set_clause = "SET " + set_clause
        
        with self.driver.session(database=self.database) as session:
            # Execute query
            query = f"""
            MATCH (source:{source_type} {{{source_primary_key}: $source_key}})
            MATCH (target:{target_type} {{{target_primary_key}: $target_key}})
            MERGE (source)-[r:{rel_type}]->(target)
            {set_clause}
            RETURN r
            """
            
            result = session.run(query, params)
            return result.single()