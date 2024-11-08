from fastapi import FastAPI, Depends, HTTPException
from fastapi.responses import JSONResponse
from .middlewares.cors import add_cors_middleware
from .routes import tables, columns, rules, metadata
# from os import getenv as env
from dotenv import load_dotenv

app = FastAPI(
    title="Fastapi Neo4j Application",
    summary="Fastapi Neo4j POC application",
    version="0.0.1",
)

load_dotenv()


app.include_router(tables.router)
app.include_router(columns.router)
app.include_router(rules.router)
app.include_router(metadata.router)


add_cors_middleware(app)

# app.include_router(sample_router)


@app.get("/")
async def root():
    return {"message": f"Hello, Fastapi Template Is Ready - {env['Environment']}"}

@app.exception_handler(HTTPException)
async def http_exception_handler(_, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail},
    )
