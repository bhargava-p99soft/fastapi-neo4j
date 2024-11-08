from fastapi import APIRouter, Depends, HTTPException
from ..models.rule import Rule
# from ....config.database import Neo4jConnection, get_db
from src.config.database import Neo4jConnection, get_db
import uuid

router = APIRouter()



@router.post("/rules/{table_id}")
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


@router.get("/rules/{table_id}")
def get_rules(table_id: uuid.UUID, db: Neo4jConnection = Depends(get_db)):

    query = """MATCH (p:Rule)-[r:rule_of]-(t:Table {table_id: $table_id})
    RETURN p
    """

    result = db.query(query, {"table_id": str(table_id)})
    
    if not result:
        raise HTTPException(status_code=400, detail="Failed to get rules")
    
    return result



@router.get("/rules/{table_id}/{rule_id}")
def get_rules(table_id: uuid.UUID,rule_id: uuid.UUID, db: Neo4jConnection = Depends(get_db)):

    query = """MATCH (p:Rule {rule_id: $rule_id})-[:rule_of]-(t:Table {table_id: $table_id})
    RETURN p
    """

    result = db.query(query, {"rule_id": str(rule_id), "table_id": str(table_id)})
    print(result)
    
    if not result:
        raise HTTPException(status_code=400, detail="Failed to get rules")
    
    return result[0][0]


@router.delete("/rules/{table_id}/{rule_id}")
def delete_rule(table_id: uuid.UUID, rule_id: uuid.UUID, db: Neo4jConnection = Depends(get_db)):

    db.query("""Match (t:Table {table_id: $table_id})-[r:rule_of]-(c:Rule {rule_id: $rule_id}) delete r""", {"table_id": str(table_id), "rule_id": str(rule_id)})

    query = """MATCH (p:Rule {rule_id: $rule_id})
    Delete p
    """

    result = db.query(query, {"rule_id": str(rule_id)})
    
    if result:
        raise HTTPException(status_code=400, detail="Failed to delete rule")
    
    
    return result

