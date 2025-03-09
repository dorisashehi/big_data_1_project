"""Class to insert and delete nodes and relationships."""
from neo4j import GraphDatabase
import pandas as pd

# Neo4j Connection Credentials
NEO4J_URI = "bolt://3.86.233.238:7687"
USERNAME = "neo4j"
PASSWORD = "landings-emergencies-generator"

# Connect to Neo4j
# Connect to Neo4j
class Neo4jDatabase:
    """class for nodes."""
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        """func to close connection"""
        self.driver.close()

    def delete_all_indices(self):
        """Fetch all constraints"""
        query = "SHOW CONSTRAINTS"
        with self.driver.session() as session:
            constraints = session.run(query)

            for constraint in constraints:
                # Check if it's a uniqueness constraint
                if constraint['type'] == 'UNIQUENESS':
                    # Extract the constraint name
                    constraint_name = constraint['name']

                    # Construct and execute the drop query
                    drop_query = f"DROP CONSTRAINT {constraint_name}"
                    session.run(drop_query)

            # drop kind index
            drop_query = "DROP INDEX node_kind_index IF EXISTS"
            session.run(drop_query)

    def drop_indexes(self):
        """Function to drop all existing indexes before creating new ones."""
        query = "SHOW INDEXES"
        with self.driver.session() as session:
            result = session.run(query)
            for record in result:
                index_name = record["name"]  # Extract index name
                drop_query = f"DROP INDEX {index_name}"
                session.run(drop_query)

    def create_indexes(self):
        """func to create connections"""
        queries = [
            "CREATE CONSTRAINT unique_id FOR (n:Node) REQUIRE n.id IS UNIQUE",
            "CREATE INDEX node_kind_index FOR (n:Node) ON (n.kind)"
        ]
        with self.driver.session() as session:
            for query in queries:
                session.run(query)

    def batch_insert_nodes(self, nodes):
        """func to insert nodes"""
        #total_nodes = len(nodes)
        #inserted_count = 0
        query = """
            UNWIND $nodes AS node
            MERGE (n:Node {id: node.id, name: node.name, kind: node.kind})
        """

        with self.driver.session() as session:
            session.run(query, nodes=nodes)
            """
            for i in range(0, total_nodes, 1000):
                print("Started to insert nodes...")
                batch = nodes[i:i + 1000]
                session.run(query, nodes=nodes)
                inserted_count += len(batch)
                print(f"Inserted {inserted_count}/{total_nodes} nodes...")
            """

    def batch_insert_relationships(self, edges, batch_size=1000):
        """Function to batch insert relationships in chunks"""
        query = """
        UNWIND $edges as edge
        MATCH (a:Node {id: edge.source}), (b:Node {id: edge.target})
        MERGE (a)-[r:RELATIONSHIP {type: edge.metaedge}]->(b)
        """

        with self.driver.session() as session:
            total_edges = len(edges)
            inserted_count = 0

            for i in range(0, total_edges, batch_size):
                batch = edges[i:i + batch_size]
                session.run(query, edges=batch)
                inserted_count += len(batch)
                print(f"Inserted {inserted_count}/{total_edges} relationships...")


    def delete_all_nodes(self):
        """
            Delete all nodes
        """
        query = (
            "MATCH (n) DELETE n"
        )
        with self.driver.session() as session:
            session.run(query)

    def delete_all_edges(self):
        """
            Delete all relationships
        """
        query = (
            "MATCH ()-[r]->() DELETE r"
        )
        with self.driver.session() as session:
            session.run(query)


# Initialize Database
db = Neo4jDatabase(NEO4J_URI, USERNAME, PASSWORD)

print("Starting to delete edges, nodes, indexes and indices.")
db.delete_all_indices()
db.drop_indexes()
db.delete_all_nodes()
db.delete_all_edges()
print("Finished to delete edges, nodes, indexes and indices.")

print("Starting to create edges, nodes, indexes and indices.")
db.create_indexes()

# Load and Insert Nodes from nodes.tsv
nodes_df = pd.read_csv("./data/nodes.tsv", sep="\t", names=["id", "name", "kind"])
nodes_list = nodes_df.to_dict("records")
db.batch_insert_nodes(nodes_list)

# Load and Insert Relationships from edges.tsv
edges_df = pd.read_csv("./data/edges.tsv", sep="\t", names=["source", "metaedge", "target"])
edges_list = edges_df.to_dict("records")
db.batch_insert_relationships(edges_list)
print("Finished to create edges, nodes, indexes and indices.")

# Close Database Connection
db.close()

print("Optimized Data Insertion Completed!")