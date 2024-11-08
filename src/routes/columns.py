from fastapi import APIRouter, Depends, HTTPException
from ..models.column import Column
# from ....config.database import Neo4jConnection, get_db
from src.config.database import Neo4jConnection, get_db

import uuid


router = APIRouter()



@router.post("/columns/{table_id}")
def create_column(column: Column, table_id: uuid.UUID, db: Neo4jConnection = Depends(get_db)):

    column_id = str(uuid.uuid4())

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


@router.get("/columns/{table_id}")
def get_columns(table_id: uuid.UUID, db: Neo4jConnection = Depends(get_db)):

    query = """MATCH (p:Column)-[r:column_of]-(t:Table {table_id: $table_id})
    RETURN p
    """

    result = db.query(query, {"table_id": str(table_id)})
    
    if not result:
        raise HTTPException(status_code=400, detail="Failed to get columns")
    
    return result



@router.get("/columns/{table_id}/{column_id}")
def get_columns(table_id: uuid.UUID,column_id: uuid.UUID, db: Neo4jConnection = Depends(get_db)):

    query = """MATCH (p:Column {column_id: $column_id})-[:column_of]-(t:Table {table_id: $table_id})
    RETURN p
    """

    result = db.query(query, {"column_id": str(column_id), "table_id": str(table_id)})
    print(result)
    
    if not result:
        raise HTTPException(status_code=400, detail="Failed to get columns")
    
    return result[0][0]


@router.delete("/columns/{table_id}/{column_id}")
def delete_column(table_id: uuid.UUID, column_id: uuid.UUID, db: Neo4jConnection = Depends(get_db)):

    db.query("""Match (t:Table {table_id: $table_id})-[r:column_of]-(c:Column {column_id: $column_id}) delete r""", {"table_id": str(table_id), "column_id": str(column_id)})

    query = """MATCH (p:Column {column_id: $column_id})
    Delete p
    """

    result = db.query(query, {"column_id": str(column_id)})
    
    if result:
        raise HTTPException(status_code=400, detail="Failed to delete column")
    
    
    return result
