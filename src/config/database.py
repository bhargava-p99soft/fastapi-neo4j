from os import getenv
from neo4j import GraphDatabase

# Neo4j connection configuration
NEO4J_URI = getenv('NEO4J_URI')  # Update this with your Neo4j URI
NEO4J_USER = getenv('NEO4J_USER')
NEO4J_PASSWORD = getenv('NEO4J_PASSWORD')

class Neo4jConnection:
    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def query(self, query: str, parameters: dict = {}):
        with self.driver.session() as session:
            result = session.run(query, parameters)
            return [record for record in result]
        

# Dependency to get a Neo4j connection
def get_db():
    db = Neo4jConnection(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    try:
        yield db
    finally:
        print("closing connection")
        db.close()