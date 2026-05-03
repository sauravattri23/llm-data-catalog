"""
=============================================================
  database.py
  Database connection shared across all routes.
=============================================================
"""

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://catalog_user:catalog_pass@localhost:5432/ecommerce_db"
)

engine        = create_engine(DATABASE_URL, echo=False)
SessionLocal  = sessionmaker(bind=engine, autocommit=False, autoflush=False)


def get_db():
    """
    Database session dependency.
    Used in FastAPI routes with Depends(get_db).
    Automatically closes session after request.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()