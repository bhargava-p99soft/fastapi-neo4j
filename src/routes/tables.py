from fastapi import APIRouter, Depends, HTTPException
from ..models.table import Table
# from ....config.database import Neo4jConnection, get_db
from src.config.database import Neo4jConnection, get_db

import uuid

router = APIRouter()

@router.post("/tables/")
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
    

@router.get("/tables/{table_id}")
def get_table(table_id: uuid.UUID, db: Neo4jConnection = Depends(get_db)):

    query = """MATCH (p:Table {table_id: $table_id})
    RETURN p
    """

    result = db.query(query, {"table_id": str(table_id)})
    
    if not result:
        raise HTTPException(status_code=400, detail="Failed to get table")
    
    
    return result[0][0]
    
@router.delete("/tables/{table_id}")
def delete_table(table_id: uuid.UUID, db: Neo4jConnection = Depends(get_db)):


    query = """MATCH (p:Table {table_id: $table_id})
    Delete p
    """
    

    result = db.query(query, {"table_id": str(table_id)})
    
    if result:
        raise HTTPException(status_code=400, detail="Failed to delete table")
    
    
    return result

    