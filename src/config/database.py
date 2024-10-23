from neo4j import GraphDatabase


class Neo4jConnection:
    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def query(self, query: str, parameters: dict = {}):
        with self.driver.session() as session:
            result = session.run(query, parameters)
            return [record for record in result]