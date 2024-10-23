from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
from neo4j import GraphDatabase
from os import getenv
from typing import Dict, Any, Optional 
import uuid
from dotenv import load_dotenv

from .api.v1.routes import tables, columns, rules


app = FastAPI(
    title="Fastapi Neo4j Application",
    summary="Fastapi Neo4j POC application",
    version="0.0.1",
)

load_dotenv()


app.include_router(tables.router)
app.include_router(columns.router)
app.include_router(rules.router)


# if __name__ == '__main__':
#     import uvicorn
#     uvicorn.run(app)

