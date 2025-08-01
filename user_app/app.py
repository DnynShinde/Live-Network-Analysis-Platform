import os
from dotenv import load_dotenv

# Import Neo4jGraph from langchain_community.graphs for compatibility
from langchain_community.graphs import Neo4jGraph
# Import GraphCypherQAChain from langchain_community.chains.graph_qa.cypher
from langchain_community.chains.graph_qa.cypher import GraphCypherQAChain
# Import ChatGoogleGenerativeAI for using Gemini Pro
from langchain_google_genai import ChatGoogleGenerativeAI
from utils.ontology_loader import load_ontology

# Load environment variables from .env file
# This will load the GOOGLE_API_KEY
load_dotenv()
api_key = os.getenv("GOOGLE_API_KEY")

# --- Neo4j Connection Setup ---
# Initialize the Neo4jGraph object.
graph = Neo4jGraph(
    url="bolt://localhost:7687",
    username="neo4j",
    password="password",
    database="neo4j"
)

# Optional: Refresh the graph schema.
graph.refresh_schema()
schema = graph.get_schema
ontology = load_ontology()

# Create an enhanced schema description with ontology details
def create_enhanced_schema():
    base_schema = graph.get_schema
    ontology_info = load_ontology()
    
    # Start with the base schema
    enhanced_schema = f"# Neo4j Graph Schema\n{base_schema}\n\n"
    
    # Add detailed node information from ontology
    enhanced_schema += "# Detailed Node Types\n"
    for node_type, props in ontology_info.get('nodes', {}).items():
        enhanced_schema += f"\n## {node_type}\n"
        
        # Add labels
        if 'labels' in props:
            enhanced_schema += f"Additional labels: {', '.join(props['labels'])}\n"
        
        # Add required properties
        if 'required_properties' in props:
            enhanced_schema += f"Required properties: {', '.join(props['required_properties'])}\n"
        
        # Add optional properties
        if 'optional_properties' in props:
            enhanced_schema += f"Optional properties: {', '.join(props['optional_properties'])}\n"
    
    # Add detailed relationship information
    enhanced_schema += "\n# Detailed Relationship Types\n"
    for rel_type, props in ontology_info.get('relationships', {}).items():
        enhanced_schema += f"\n## {rel_type}\n"
        enhanced_schema += f"Source: {props.get('source')}\n"
        enhanced_schema += f"Target: {props.get('target')}\n"
        
        # Add required properties
        if 'required_properties' in props:
            enhanced_schema += f"Required properties: {', '.join(props['required_properties'])}\n"
        
        # Add optional properties
        if 'optional_properties' in props:
            enhanced_schema += f"Optional properties: {', '.join(props['optional_properties'])}\n"
    
    return enhanced_schema

# Reduce the context window by trimming the schema
def create_simplified_schema():
    # Include only the most relevant parts of the ontology
    # Focus on node/relationship types and required properties
    # ...
    base_schema = graph.get_schema
    ontology_info = load_ontology()
    
    # Start with the base schema
    simplified_schema = f"# Neo4j Graph Schema\n{base_schema}\n\n"
    
    # Add simplified node information from ontology
    simplified_schema += "# Node Types\n"
    for node_type, props in ontology_info.get('nodes', {}).items():
        simplified_schema += f"\n## {node_type}\n"
        
        # Add required properties
        if 'required_properties' in props:
            simplified_schema += f"Required properties: {', '.join(props['required_properties'])}\n"
    
    # Add simplified relationship information
    simplified_schema += "\n# Relationship Types\n"
    for rel_type, props in ontology_info.get('relationships', {}).items():
        simplified_schema += f"\n## {rel_type}\n"
        enhanced_schema += f"Source: {props.get('source')}\n"
        enhanced_schema += f"Target: {props.get('target')}\n"
        
        # Add required properties
        if 'required_properties' in props:
            simplified_schema += f"Required properties: {', '.join(props['required_properties'])}\n"
    
    return simplified_schema

# Cache the schema refresh
schema = graph.get_schema
enhanced_schema = create_enhanced_schema()  # Do this once at startup

# --- LLM (Gemini) Setup ---
llm = ChatGoogleGenerativeAI(
    model="gemini-1.5-flash",
    temperature=0,
    google_api_key=api_key
)

# --- GraphCypherQAChain Setup with Enhanced Schema ---
chain = GraphCypherQAChain.from_llm(
    llm=llm,
    graph=graph,
    verbose=False,
    return_intermediate_steps=True,
    allow_dangerous_requests=True,
    top_k=5,  # Reduced from 10
)

def process_query(query_text):
    """Process a natural language query and display only the Cypher query and final response."""
    # print("\n" + "="*80)
    print(f"\nNatural Language Query: {query_text}")
    # print("="*80)
    
    # Add context from enhanced schema to the query
    contextual_query = f"""
Use the following Neo4j graph schema to answer this question:
{enhanced_schema}

Now, please answer this question about the network graph: {query_text}
"""
    
    # Show the schema being used (optional, for debugging)
    # print("\n Using Schema Context:")
    # print("-"*80)
    # print(enhanced_schema)
    
    # Invoke the chain with the contextual query
    response = chain.invoke({"query": contextual_query})
    
    # Extract components from the response
    cypher_query = response.get("intermediate_steps", [{}])[0].get("query", "No Cypher query generated")
    # cypher_result = response.get("intermediate_steps", [{}])[0].get("result", "No results from Neo4j")
    final_answer = response.get("result", "No answer generated")
    
    # Display only the Cypher query
    print("\nCypher Query:")
    print(cypher_query)
    
    # Comment out Neo4j query results display
    # print("\n Neo4j Query Results:")
    # print("-"*80)
    # if isinstance(cypher_result, list):
    #     if not cypher_result:
    #         print("No results returned from database")
    #     else:
    #         for i, item in enumerate(cypher_result):
    #             print(f"Result {i+1}:")
    #             if isinstance(item, dict):
    #                 for key, value in item.items():
    #                     print(f"  {key}: {value}")
    #             else:
    #                 print(f"  {item}")
    # else:
    #     print(cypher_result)
    
    # Display only the final LLM response
    print("\nResponse:")
    print(final_answer)
    # print("="*80)
    
    return final_answer

def interactive_mode():
    """Run in interactive mode where user can enter multiple queries."""
    print("\nWelcome to the Network Graph Query System")
    print("Enter your questions about the network or type 'exit' to quit")
    
    while True:
        try:
            query = input("\nYour question: ")
            if query.lower() in ['exit', 'quit', 'q']:
                print("Goodbye!")
                break
            
            if not query.strip():
                continue
                
            process_query(query)
            
        except KeyboardInterrupt:
            print("\nGoodbye!")
            break
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    interactive_mode()