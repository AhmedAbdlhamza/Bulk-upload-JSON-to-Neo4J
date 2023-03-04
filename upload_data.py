from neo4j import GraphDatabase
import json

# Connect to the Neo4j database
uri = "bolt://localhost:7687"
user = "neo4j"
password = "your_password"
driver = GraphDatabase.driver(uri, auth=(user, password))

# Load the JSON data from the file
with open('data.json', 'r') as f:
    data = json.load(f)

# Define a function to create nodes
def create_node(tx, label, properties):
    query = f"CREATE (n:{label} {properties})"
    tx.run(query)

# Define a function to create relationships
def create_relationship(tx, node1_label, node1_id, rel_type, node2_label, node2_id, properties):
    query = f"MATCH (n1:{node1_label} {{id: '{node1_id}'}}), (n2:{node2_label} {{id: '{node2_id}'}}) CREATE (n1)-[r:{rel_type} {properties}]->(n2)"
    tx.run(query)

# Process the data and create the nodes and relationships
with driver.session() as session:
    for row in data:
        # Create person node
        person_props = json.dumps(row['person'])
        session.write_transaction(create_node, 'Person', person_props)

        # Create company node
        company_props = json.dumps(row['company'])
        session.write_transaction(create_node, 'Company', company_props)

        # Create education node
        education_props = json.dumps(row['education'])
        session.write_transaction(create_node, 'Education', education_props)

        # Create location nodes
        locality_props = json.dumps(row['locality'])
        session.write_transaction(create_node, 'Location', locality_props)

        region_props = json.dumps(row['region'])
        session.write_transaction(create_node, 'Location', region_props)

        country_props = json.dumps(row['country'])
        session.write_transaction(create_node, 'Location', country_props)

        metro_props = json.dumps(row['metro'])
        session.write_transaction(create_node, 'Location', metro_props)

        # Create relationships
        session.write_transaction(create_relationship, 'Person', row['person']['id'], 'works_at', 'Company', row['company']['id'], json.dumps({}))
        session.write_transaction(create_relationship, 'Person', row['person']['id'], 'worked_at', 'Company', row['company']['id'], json.dumps({'duration': row['duration']}))
        session.write_transaction(create_relationship, 'Person', row['person']['id'], 'studied_at', 'Education', row['education']['id'], json.dumps({'degree': row['education']['degree'], 'field_of_study': row['education']['field_of_study']}))
        session.write_transaction(create_relationship, 'Location', row['school_location']['id'], 'located_at', 'Education', row['education']['id'], json.dumps({}))
        session.write_transaction(create_relationship, 'Location', row['company_location']['id'], 'located_at', 'Company', row['company']['id'], json.dumps({}))
        session.write_transaction(create_relationship, 'Location', row['person_location']['id'], 'located_at', 'Person', row['person']['id'], json.dumps({}))
