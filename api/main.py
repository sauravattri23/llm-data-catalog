"""
=============================================================
  main.py
  FastAPI application entry point.
  Registers all routes and starts the server.
=============================================================
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

from routes import tables, columns, search, lineage, quality

load_dotenv()

# ─────────────────────────────────────────────
#  CREATE FASTAPI APP
# ─────────────────────────────────────────────
app = FastAPI(
    title       = "LLM Data Catalog API",
    description = """
    REST API for the LLM-Powered Data Catalog.

    ## Features
    - 📋 Browse all tables and columns
    - 🔍 Search catalog by keyword
    - 🔗 Explore data lineage
    - 📊 View data quality scores
    - 🤖 AI-generated descriptions
    """,
    version     = "1.0.0",
    docs_url    = "/docs",      # Swagger UI
    redoc_url   = "/redoc",     # ReDoc UI
)

# ─────────────────────────────────────────────
#  CORS — Allow React frontend to call API
# ─────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins     = ["*"],   # allow all origins in dev
    allow_credentials = True,
    allow_methods     = ["*"],
    allow_headers     = ["*"],
)

# ─────────────────────────────────────────────
#  REGISTER ROUTES
# ─────────────────────────────────────────────
app.include_router(tables.router,  prefix="/tables",  tags=["Tables"])
app.include_router(columns.router, prefix="/columns", tags=["Columns"])
app.include_router(search.router,  prefix="/search",  tags=["Search"])
app.include_router(lineage.router, prefix="/lineage", tags=["Lineage"])
app.include_router(quality.router, prefix="/quality", tags=["Quality"])


# ─────────────────────────────────────────────
#  ROOT ENDPOINT
# ─────────────────────────────────────────────
@app.get("/", tags=["Root"])
def root():
    """Welcome endpoint — shows API info."""
    return {
        "name"       : "LLM Data Catalog API",
        "version"    : "1.0.0",
        "description": "AI-powered data catalog REST API",
        "docs"       : "http://localhost:8000/docs",
        "endpoints"  : {
            "tables" : "http://localhost:8000/tables",
            "search" : "http://localhost:8000/search",
            "lineage": "http://localhost:8000/lineage",
            "quality": "http://localhost:8000/quality",
        }
    }


# ─────────────────────────────────────────────
#  HEALTH CHECK
# ─────────────────────────────────────────────
@app.get("/health", tags=["Root"])
def health_check():
    """API health check endpoint."""
    return {
        "status" : "healthy",
        "api"    : "running",
    }


# ─────────────────────────────────────────────
#  RUN SERVER
# ─────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host     = "0.0.0.0",
        port     = 8000,
        reload   = True,    # auto-reload on code changes
    )