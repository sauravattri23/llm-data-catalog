"""
=============================================================
  routes/columns.py
  Column-related API endpoints.
=============================================================
"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Optional
import sys
sys.path.insert(0, '..')
from database import get_db

router = APIRouter()


@router.get("/")
def get_all_columns(
    db: Session = Depends(get_db),
    table_name: Optional[str] = None,
    col_type: Optional[str]   = None,
    limit: Optional[int]      = 100
):
    """
    Get all columns in the catalog.
    Optionally filter by table name or column type.
    """
    query = """
        SELECT
            table_name,
            column_name,
            clean_type,
            is_nullable,
            is_primary_key,
            is_foreign_key,
            references_table,
            null_pct,
            ai_description
        FROM catalog_columns
        WHERE 1=1
    """
    params = {"limit": limit}

    if table_name:
        query += " AND table_name = :table_name"
        params["table_name"] = table_name

    if col_type:
        query += " AND clean_type = :col_type"
        params["col_type"] = col_type

    query += " ORDER BY table_name, column_name LIMIT :limit"

    result = db.execute(text(query), params)
    rows   = result.fetchall()

    return {
        "total"  : len(rows),
        "columns": [dict(row._mapping) for row in rows]
    }


@router.get("/foreign-keys")
def get_foreign_key_columns(db: Session = Depends(get_db)):
    """
    Get all foreign key columns across all tables.
    Shows the complete relationship map at column level.
    """
    result = db.execute(text("""
        SELECT
            table_name,
            column_name,
            references_table,
            references_column,
            ai_description
        FROM catalog_columns
        WHERE is_foreign_key = 'True'
        ORDER BY table_name, column_name
    """))

    rows = result.fetchall()
    return {
        "total"      : len(rows),
        "foreign_keys": [dict(row._mapping) for row in rows]
    }


@router.get("/primary-keys")
def get_primary_key_columns(db: Session = Depends(get_db)):
    """
    Get all primary key columns across all tables.
    """
    result = db.execute(text("""
        SELECT
            table_name,
            column_name,
            clean_type,
            ai_description
        FROM catalog_columns
        WHERE is_primary_key = 'True'
        ORDER BY table_name
    """))

    rows = result.fetchall()
    return {
        "total"       : len(rows),
        "primary_keys": [dict(row._mapping) for row in rows]
    }


@router.get("/types")
def get_column_type_distribution(db: Session = Depends(get_db)):
    """
    Get distribution of column data types.
    Shows how many columns of each type exist.
    """
    result = db.execute(text("""
        SELECT
            clean_type,
            COUNT(*) AS count
        FROM catalog_columns
        GROUP BY clean_type
        ORDER BY count DESC
    """))

    rows = result.fetchall()
    return {
        "type_distribution": [dict(row._mapping) for row in rows]
    }


@router.get("/{table_name}/{column_name}")
def get_column(
    table_name : str,
    column_name: str,
    db: Session = Depends(get_db)
):
    """
    Get full details for a specific column.
    """
    result = db.execute(text("""
        SELECT
            table_name,
            column_name,
            clean_type,
            raw_type,
            max_length,
            is_nullable,
            is_primary_key,
            is_foreign_key,
            references_table,
            references_column,
            null_count,
            null_pct,
            ai_description,
            last_crawled_at
        FROM catalog_columns
        WHERE table_name  = :table_name
          AND column_name = :column_name
    """), {
        "table_name" : table_name,
        "column_name": column_name
    })

    row = result.fetchone()
    if not row:
        raise HTTPException(
            status_code = 404,
            detail      = f"Column '{table_name}.{column_name}' not found"
        )

    return dict(row._mapping)