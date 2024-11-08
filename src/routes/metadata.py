from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from typing import List, Dict, Any
import os
from dotenv import load_dotenv
from src.config.database import Neo4jConnection, get_db
from ..utils.idgenerator import generate_custom_id
from uuid import UUID
from ..models.metadata import SearchResponse

router = APIRouter()
load_dotenv()


# Snowflake connection configuration
SNOWFLAKE_USER = os.getenv("user")
SNOWFLAKE_PASSWORD = os.getenv("password")
SNOWFLAKE_ACCOUNT = os.getenv("account")  # e.g., 'xy12345.snowflakecomputing.com'
# SNOWFLAKE_WAREHOUSE = "your_warehouse"


# SQLAlchemy connection string for Snowflake
DATABASE_URL = (
    f"snowflake://{SNOWFLAKE_USER}:{SNOWFLAKE_PASSWORD}@{SNOWFLAKE_ACCOUNT}/"
    # "?warehouse=" + SNOWFLAKE_WAREHOUSE
)

# Create an SQLAlchemy engine with connection pooling
engine = create_engine(
    DATABASE_URL,
    pool_size=5,
    max_overflow=10,
    pool_timeout=30,
    pool_recycle=1800
)

# Create a session factory bound to the engine
SessionLocal = sessionmaker(bind=engine)

# Function to execute a metadata query
def execute_metadata_query(query: str) -> List[Dict[str, Any]]:
    try:
        with SessionLocal() as session:
            result = session.execute(text(query))

            # Check if rows are named tuples (use _asdict())
            try:
                metadata = [row._asdict() for row in result.fetchall()]
            except AttributeError:
                # Fallback to manual mapping if _asdict() is unavailable
                metadata = [dict(row) for row in result.mappings().all()]

        return metadata
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error executing metadata query: {e}")


# Endpoint to get all databases
@router.get("/snowflake/databases", response_model=List[Dict[str, Any]])
async def get_databases():
    query = "SHOW DATABASES"
    return execute_metadata_query(query)

# Endpoint to get all schemas in a specific database
@router.get("/snowflake/schemas/{database}", response_model=List[Dict[str, Any]])
async def get_schemas(database: str):
    query = f"SHOW SCHEMAS IN DATABASE {database}"
    return execute_metadata_query(query)

# Endpoint to get all tables in a specific schema
@router.get("/snowflake/tables/{database}/{schema}", response_model=List[Dict[str, Any]])
async def get_tables(database: str, schema: str):
    query = f"SHOW TABLES IN SCHEMA {database}.{schema}"
    return execute_metadata_query(query)

# Endpoint to get all columns in a specific table
@router.get("/snowflake/columns/{database}/{schema}/{table}", response_model=List[Dict[str, Any]])
async def get_columns(database: str, schema: str, table: str):
    query = f"SHOW COLUMNS IN TABLE {database}.{schema}.{table}"
    return execute_metadata_query(query)

# Endpoint to get all roles
@router.get("/snowflake/roles", response_model=List[Dict[str, Any]])
async def get_roles():
    query = "SHOW ROLES"
    return execute_metadata_query(query)

# Endpoint to get all users
@router.get("/snowflake/users", response_model=List[Dict[str, Any]])
async def get_users():
    query = "SHOW USERS"
    return execute_metadata_query(query)

# Endpoint to get all warehouses
@router.get("/snowflake/warehouses", response_model=List[Dict[str, Any]])
async def get_warehouses():
    query = "SHOW WAREHOUSES"
    return execute_metadata_query(query)



# Function to get all tables and column metadata in all schemas within a database
def get_all_tables_with_full_column_metadata(database: str) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    all_schemas_data = {}
    try:
        with SessionLocal() as session:
            # Step 1: Get all schemas in the specified database
            schemas_query = text(f"SHOW SCHEMAS IN DATABASE {database}")
            schemas_result = session.execute(schemas_query)
            schemas = [row[1] for row in schemas_result.fetchall()]  # Schema name is in the second column

            # Step 2: For each schema, get all tables and their columns
            for schema in schemas:
                schema_data = {}
                
                # Get all tables in the current schema
                tables_query = text(f"SHOW TABLES IN SCHEMA {database}.{schema}")
                tables_result = session.execute(tables_query)
                tables = [row[1] for row in tables_result.fetchall()]  # Table name is in the second column
                
                # Step 3: For each table, get full column metadata
                for table in tables:
                    columns_query = text(f"SHOW COLUMNS IN TABLE {database}.{schema}.{table}")
                    columns_result = session.execute(columns_query)
                    
                    # Collect all column properties dynamically
                    columns_metadata = []
                    for row in columns_result.mappings().all():
                        # Convert each row to a dictionary of all available column properties
                        column_info = {key: value for key, value in row.items()}
                        columns_metadata.append(column_info)
                    
                    # Add table's full column metadata to the schema data
                    schema_data[table] = columns_metadata

                # Add schema data to the main dictionary
                all_schemas_data[schema] = schema_data

        return all_schemas_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving tables and columns: {e}")
    

# Endpoint to get all tables with column metadata in all schemas in a specified database
@router.get("/extract-metadata/{database}", response_model=Dict[str, Dict[str, List[Dict[str, Any]]]])
async def get_tables_with_columns(database: str):
    return get_all_tables_with_full_column_metadata(database)


def save_to_neo4j(database: str, metadata: Dict[str, Dict[str, List[Dict[str, Any]]]], db: Neo4jConnection):
    try:
        # Generate custom_id for the Database node
        db_custom_id = generate_custom_id()
        db.query("MERGE (db:Database {name: $database, custom_id: $custom_id})", {"database": database, "custom_id": db_custom_id})
        
        # Iterate through schemas, tables, and columns to create nodes and relationships
        for schema_name, tables in metadata.items():
            schema_custom_id = generate_custom_id()
            db.query(
                """
                MATCH (db:Database {name: $database})
                MERGE (s:Schema {name: $schema_name, custom_id: $schema_custom_id})
                MERGE (db)-[:CONTAINS]->(s)
                """,
                {"database": database, "schema_name": schema_name, "schema_custom_id": schema_custom_id}
            )
            
            for table_name, columns in tables.items():
                table_custom_id = generate_custom_id()
                db.query(
                    """
                    MATCH (s:Schema {name: $schema_name})
                    MERGE (t:Table {name: $table_name, custom_id: $table_custom_id})
                    MERGE (s)-[:CONTAINS]->(t)
                    """,
                    {"schema_name": schema_name, "table_name": table_name, "table_custom_id": table_custom_id}
                )
                
                for column in columns:
                    column_name = column.get("column_name")
                    column_properties = {key: column[key] for key in column if column[key] is not None}
                    column_custom_id = generate_custom_id()
                    
                    db.query(
                        """
                        MATCH (t:Table {name: $table_name})
                        MERGE (c:Column {name: $column_name, custom_id: $column_custom_id})
                        SET c += $column_properties
                        MERGE (t)-[:CONTAINS]->(c)
                        """,
                        {
                            "table_name": table_name,
                            "column_name": column_name,
                            "column_custom_id": column_custom_id,
                            "column_properties": column_properties
                        }
                    )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error persisting metadata to Neo4j: {e}")





# Endpoint to retrieve metadata and save it to Neo4j
@router.post("/persist-metadata/{database}")
async def persist_metadata(database: str, db: Neo4jConnection = Depends(get_db)):
    try:
        # Retrieve metadata from Snowflake
        metadata = get_all_tables_with_full_column_metadata(database)
        
        # Persist metadata to Neo4j
        save_to_neo4j(database, metadata, db)
        
        return {"message": "Metadata persisted successfully to Neo4j"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error persisting metadata: {e}")



@router.put("/add_metadata/{node_id}")
async def update_node_properties(
    node_id: UUID, 
    properties: Dict[str, Any], 
    db: Neo4jConnection = Depends(get_db)
):
    try:
        # Prepare the Cypher query to update node properties based on custom_id
        query = """
        MATCH (n)
        WHERE n.custom_id = $custom_id
        SET n += $properties
        RETURN n
        """
        
        # Execute the query to update properties on the node
        result = db.query(query, {"custom_id": str(node_id), "properties": properties})
        
        # Check if the node exists and was updated
        if not result:
            raise HTTPException(status_code=404, detail="Node not found")
        
        # Return the updated node data
        return {"message": "Node properties updated successfully", "node": result[0]}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error updating node: {e}")



@router.get('/search-nodes/')
async def search_nodes(keyword: str, db: Neo4jConnection = Depends(get_db)):
    try:
        # Convert the keyword to lowercase for case-insensitive search
        keyword_lower = keyword.lower()

        # Cypher query to search nodes where any property contains the keyword, case-insensitively
        query = """
        MATCH (n)
        WHERE any(property IN keys(n) 
                  WHERE toLower(toString(n[property])) CONTAINS toLower($keyword))
        RETURN n
        """
        
        # Execute the query with the case-insensitive keyword
        result = db.query(query, {"keyword": keyword_lower})
        
        # If no nodes are found, return an empty list
        if not result:
            return SearchResponse(message="No nodes found matching the keyword", nodes=[])
        
        # Format the result to return the raw nodes as dictionaries
        nodes = []
        for record in result:
            node = record.get("n")
            
            # Convert node to a dictionary and include all fields (extra fields included automatically)
            node_data = dict(node)
            
            # Add the node to the list of nodes
            nodes.append(node_data)
        
        return SearchResponse(message="Nodes found", nodes=nodes)
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error searching nodes: {e}")




@router.get("/search-tables-with-rules", response_model=SearchResponse)
async def search_tables_with_rules(keyword: str, db: Neo4jConnection = Depends(get_db)):
    try:
        # Convert the keyword to lowercase for case-insensitive search
        keyword_lower = keyword.lower()

        # Cypher query to search for `Table` nodes containing the keyword and their related nodes through `rule_of` relationship
        query = """
        MATCH (t:Table)
        WHERE any(property IN keys(t) 
                  WHERE toLower(toString(t[property])) CONTAINS toLower($keyword))
        OPTIONAL MATCH (r)-[:rule_of]->(t)
        RETURN t, collect(r) AS rules
        """
        
        # Execute the query with the case-insensitive keyword
        result = db.query(query, {"keyword": keyword_lower})
        
        # If no nodes are found, return an empty list
        if not result:
            return SearchResponse(message="No matching tables found", nodes=[])

        # Format the result to return the `Table` nodes and their related `Rule` nodes
        nodes = []
        for record in result:
            table_node = record.get("t")
            rules = record.get("rules", [])

            # Convert the `Table` node to a dictionary and add related rules
            table_data = {
                "table": dict(table_node),  # Convert the table node to a dictionary
                "rules": [dict(rule) for rule in rules]  # Convert each rule node to a dictionary
            }
            
            # Add the table data to the nodes list
            nodes.append(table_data)

        return SearchResponse(message="Tables and related rules found", nodes=nodes)
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error searching tables and rules: {e}")
