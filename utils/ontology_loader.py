import yaml
import os

def load_ontology():
    """Load the ontology configuration from YAML file."""
    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 
                               'config', 'ontology.yaml')
    
    with open(config_path, 'r') as file:
        ontology = yaml.safe_load(file)
    
    return ontology

def get_node_properties(node_type):
    """Get required and optional properties for a node type."""
    ontology = load_ontology()
    
    if node_type not in ontology['nodes']:
        raise ValueError(f"Node type '{node_type}' not defined in ontology")
    
    node_def = ontology['nodes'][node_type]
    return {
        'required': node_def.get('required_properties', []),
        'optional': node_def.get('optional_properties', []),
        'labels': node_def.get('labels', [])
    }

def get_relationship_properties(rel_type):
    """Get required and optional properties for a relationship type."""
    ontology = load_ontology()
    
    if rel_type not in ontology['relationships']:
        raise ValueError(f"Relationship type '{rel_type}' not defined in ontology")
    
    rel_def = ontology['relationships'][rel_type]
    return {
        'source': rel_def.get('source'),
        'target': rel_def.get('target'),
        'required': rel_def.get('required_properties', []),
        'optional': rel_def.get('optional_properties', [])
    }