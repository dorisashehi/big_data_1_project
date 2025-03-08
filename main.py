"""
This file contains intructions to create database connection and credentials
"""
from neo4j import GraphDatabase
import pandas as pd

# Neo4j Connection Credentials
NEO4J_URI = "bolt://35.170.68.74:7687"
USERNAME = "neo4j"
PASSWORD = "pushup-washes-consequence"

# Connect to Neo4j
class Neo4jDatabase:
    """
       Class to initialize database connections and to make insertion.
    """
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        """
            Func to close connection
        """
        self.driver.close()

    def insert_node(self, node_id, name, category):
        """
            Func to insert node into the graph
        """
        query = (
            f"MERGE (n:{category} {{id: $id, name: $name}})"
        )
        with self.driver.session() as session:
            session.run(query, id=node_id, name=name)

    def insert_relationship(self, source, target, relation_type):
        """
            Insert relationship
        """
        query = (
            f"MATCH (a {{id: $source}}), (b {{id: $target}}) "
            f"MERGE (a)-[:{relation_type}]->(b)"
        )
        with self.driver.session() as session:
            session.run(query, source=source, target=target)

    def delete_all(self):
        """
            Delete all nodes and relationships
        """
        query = (
            "MATCH (n) "
            "DETACH DELETE n"
        )
        with self.driver.session() as session:
            session.run(query)

# Initialize Database
db = Neo4jDatabase(NEO4J_URI, USERNAME, PASSWORD)

#db.delete_all()

# Load and Insert Nodes from nodes.tsv

print("Data insertion into Neo4j in progress!")
"""
nodes_df = pd.read_csv("./data/nodes.tsv", sep="\t", header=None, names=["id", "name", "kind"])
for _, row in nodes_df.iterrows():
    db.insert_node(row["id"], row["name"], row["kind"])

"""
# Load and Insert Relationships from edges.tsv
edges_df = pd.read_csv("./data/edges.tsv", sep="\t", header=None, names=["source", "metaedge", "target"])
for _, row in edges_df.iterrows():
    db.insert_relationship(row["source"], row["target"], row["metaedge"])

print("Data insertion into Neo4j in progress!")

# Close Database Connection
db.close()

print("Data successfully inserted into Neo4j!")
