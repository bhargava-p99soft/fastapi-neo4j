from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
from neo4j import GraphDatabase
from os import environ as env
from typing import Dict, Any, Optional 
import uuid
app = FastAPI()

# Neo4j connection configuration
NEO4J_URI = env['NEO4J_URI']  # Update this with your Neo4j URI
NEO4J_USER = env['NEO4J_USER']
NEO4J_PASSWORD = env['NEO4J_PASSWORD']

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


class Table(BaseModel):
    table_id: Optional[str] = None
    name: str
    dynamic_properties: Dict[str, Any] = {}

    class Config:
        json_schema_extra = {
            "example": {
                "name": "Products",
                "dynamic_properties": {
                    "no_of_colums": 4
                }
            }
        }

class Column(BaseModel):
    column_id: Optional[str] = None
    name: str
    contextual_description: str
    dynamic_properties: Dict[str, Any] = {}

    class Config:
        json_schema_extra = {
           "example": {
                "name": "price",
                "contextual_description": "This is the product price",
                "dynamic_properties": {
                    "type": "varchar",
                    "required": "boolean"
                }
           }
        }

class Rule(BaseModel):
    rule_id: Optional[str] = None
    name: str
    contextual_description: str
    dynamic_properties: Dict[str, Any] = {}

    class Config:
        json_schema_extra = {
           "example": {
                "name": "price",
                "contextual_description": "This is the product price",
                "dynamic_properties": {
                    "type": "varchar",
                    "required": "boolean"
                }
           }
        }


@app.post("/tables/")
def create_table(table: Table, db: Neo4jConnection = Depends(get_db)):

    table_id = str(uuid.uuid4())

    query = """CREATE (p:Table {name: $name, table_id: $table_id})
    SET p += $dynamic_properties
    RETURN p.table_id, p.name"""
    dynamic_properties = {k: v for k, v in table.dynamic_properties.items() if isinstance(v, (str, int, float, bool, list))}

    result = db.query(query, {"table_id": table_id, "name": table.name, "dynamic_properties": dynamic_properties})
    
    if not result:
        raise HTTPException(status_code=400, detail="Failed to create table")
    
    
    return {"table_id": result[0]["p.table_id"],"name": result[0]["p.name"], **dynamic_properties}
    # # return {"table_id": result[0]["p.table_id"],"name": result[0]["p.name"], "dynamic_properties": dynamic_properties}
    # return result[0]

@app.get("/tables/{table_id}")
def get_table(table_id: uuid.UUID, db: Neo4jConnection = Depends(get_db)):

    # table_id = str(uuid.uuid4())

    query = """MATCH (p:Table {table_id: $table_id})
    RETURN p
    """
    # dynamic_properties = {k: v for k, v in table.dynamic_properties.items() if isinstance(v, (str, int, float, bool, list))}

    result = db.query(query, {"table_id": str(table_id)})
    
    if not result:
        raise HTTPException(status_code=400, detail="Failed to get table")
    
    
    return result[0][0]
    
@app.delete("/tables/{table_id}")
def delete_table(table_id: uuid.UUID, db: Neo4jConnection = Depends(get_db)):

    # table_id = str(uuid.uuid4())

    # db.query("""Match (t:Table {table_id: $table_id})-[r:column_of]-(c:Column)}) delete c""", {"table_id": str(table_id)})


    query = """MATCH (p:Table {table_id: $table_id})
    Delete p
    """
    # dynamic_properties = {k: v for k, v in table.dynamic_properties.items() if isinstance(v, (str, int, float, bool, list))}

    result = db.query(query, {"table_id": str(table_id)})
    
    if result:
        raise HTTPException(status_code=400, detail="Failed to delete table")
    
    
    return result


@app.post("/columns/{table_id}")
def create_column(column: Column, table_id: uuid.UUID, db: Neo4jConnection = Depends(get_db)):

    column_id = str(uuid.uuid4())

    # query = """CREATE (p:Column {name: $name, column_id: $column_id, contextual_description: $contextual_description})
    # SET p += $dynamic_properties
    # RETURN p.column_id, p.name, p.contextual_description"""

    query = """
        MATCH (t: Table {table_id: $table_id}) 
        CREATE (t) <-[r: column_of]- (c: Column {name: $name, column_id: $column_id, contextual_description: $contextual_description}) 
        SET c += $dynamic_properties
        RETURN c.name, c.column_id, c.contextual_description;
    """

    dynamic_properties = {k: v for k, v in column.dynamic_properties.items() if isinstance(v, (str, int, float, bool, list))}

    result = db.query(query, {"table_id": str(table_id), "column_id": column_id, "name": column.name, "contextual_description": column.contextual_description, "dynamic_properties": dynamic_properties})
    
    if not result:
        raise HTTPException(status_code=400, detail="Failed to create column")
    
    return {"column_id": result[0]["c.column_id"],"name": result[0]["c.name"], "contextual_description": result[0]["c.contextual_description"], **dynamic_properties}


@app.get("/columns/{table_id}")
def get_columns(table_id: uuid.UUID, db: Neo4jConnection = Depends(get_db)):

    query = """MATCH (p:Column)-[r:column_of]-(t:Table {table_id: $table_id})
    RETURN p
    """

    result = db.query(query, {"table_id": str(table_id)})
    
    if not result:
        raise HTTPException(status_code=400, detail="Failed to get columns")
    
    return result



@app.get("/columns/{table_id}/{column_id}")
def get_columns(table_id: uuid.UUID,column_id: uuid.UUID, db: Neo4jConnection = Depends(get_db)):

    query = """MATCH (p:Column {column_id: $column_id})-[:column_of]-(t:Table {table_id: $table_id})
    RETURN p
    """

    result = db.query(query, {"column_id": str(column_id), "table_id": str(table_id)})
    print(result)
    
    if not result:
        raise HTTPException(status_code=400, detail="Failed to get columns")
    
    return result[0][0]


@app.delete("/columns/{table_id}/{column_id}")
def delete_column(table_id: uuid.UUID, column_id: uuid.UUID, db: Neo4jConnection = Depends(get_db)):

    # if(delete_relations("Column", "column_of", db) is False):
    #     raise HTTPException(status_code=400, detail="Failed to delete column with relations")

    db.query("""Match (t:Table {table_id: $table_id})-[r:column_of]-(c:Column {column_id: $column_id}) delete r""", {"table_id": str(table_id), "column_id": str(column_id)})

    query = """MATCH (p:Column {column_id: $column_id})
    Delete p
    """

    result = db.query(query, {"column_id": str(column_id)})
    
    if result:
        raise HTTPException(status_code=400, detail="Failed to delete column")
    
    
    return result


@app.post("/rules/{table_id}")
def create_rule(rule: Rule, table_id: uuid.UUID, db: Neo4jConnection = Depends(get_db)):

    rule_id = str(uuid.uuid4())

    # query = """CREATE (p:Rule {name: $name, rule_id: $rule_id, contextual_description: $contextual_description})
    # SET p += $dynamic_properties
    # RETURN p.rule_id, p.name, p.contextual_description"""

    query = """
        MATCH (t: Table {table_id: $table_id}) 
        CREATE (t) <-[r: rule_of]- (c: Rule {name: $name, rule_id: $rule_id, contextual_description: $contextual_description}) 
        SET c += $dynamic_properties
        RETURN c.name, c.rule_id, c.contextual_description;
    """

    dynamic_properties = {k: v for k, v in rule.dynamic_properties.items() if isinstance(v, (str, int, float, bool, list))}

    result = db.query(query, {"table_id": str(table_id), "rule_id": rule_id, "name": rule.name, "contextual_description": rule.contextual_description, "dynamic_properties": dynamic_properties})
    
    if not result:
        raise HTTPException(status_code=400, detail="Failed to create rule")
    
    return {"rule_id": result[0]["c.rule_id"],"name": result[0]["c.name"], "contextual_description": result[0]["c.contextual_description"], **dynamic_properties}


@app.get("/rules/{table_id}")
def get_rules(table_id: uuid.UUID, db: Neo4jConnection = Depends(get_db)):

    query = """MATCH (p:Rule)-[r:rule_of]-(t:Table {table_id: $table_id})
    RETURN p
    """

    result = db.query(query, {"table_id": str(table_id)})
    
    if not result:
        raise HTTPException(status_code=400, detail="Failed to get rules")
    
    return result



@app.get("/rules/{table_id}/{rule_id}")
def get_rules(table_id: uuid.UUID,rule_id: uuid.UUID, db: Neo4jConnection = Depends(get_db)):

    query = """MATCH (p:Rule {rule_id: $rule_id})-[:rule_of]-(t:Table {table_id: $table_id})
    RETURN p
    """

    result = db.query(query, {"rule_id": str(rule_id), "table_id": str(table_id)})
    print(result)
    
    if not result:
        raise HTTPException(status_code=400, detail="Failed to get rules")
    
    return result[0][0]


@app.delete("/rules/{table_id}/{rule_id}")
def delete_rule(table_id: uuid.UUID, rule_id: uuid.UUID, db: Neo4jConnection = Depends(get_db)):

    # if(delete_relations("Rule", "rule_of", db) is False):
    #     raise HTTPException(status_code=400, detail="Failed to delete rule with relations")

    db.query("""Match (t:Table {table_id: $table_id})-[r:rule_of]-(c:Rule {rule_id: $rule_id}) delete r""", {"table_id": str(table_id), "rule_id": str(rule_id)})

    query = """MATCH (p:Rule {rule_id: $rule_id})
    Delete p
    """

    result = db.query(query, {"rule_id": str(rule_id)})
    
    if result:
        raise HTTPException(status_code=400, detail="Failed to delete rule")
    
    
    return result



# def delete_relations(deletion_label:str, deletion_id_title: str, deletion_id_value:str, relation: str, db: Neo4jConnection = Depends(get_db)):
#     query = """Match (a:  {$deletion_id_title})-[r:{relation: $relation}]-(b}) delete r"""
#     result = db.query(query, {"deletion_item": deletion_item, "relation": relation})
#     if result:
#         return False
#     else:
#         return True



# if __name__ == '__main__':
#     import uvicorn
#     uvicorn.run(app)


# data = {
#     "a": "this should be a",
#     "b": {
#         "c": {
#             "d": "this should be b.c.d",
#         },
#         "e": "this should be b.e"
#     }
# }

# output = {
#     "a": "this should be a",
#     "b.c.d": "this should be b.c.d", 
#     "b.e": "this should be b.e"
# }