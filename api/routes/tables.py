"""
=============================================================
  routes/tables.py
  Table-related API endpoints.
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
def get_all_tables(
    db: Session = Depends(get_db),
    order_by: Optional[str] = "quality_score",
    limit: Optional[int] = 50
):
    """
    Get all tables in the catalog.

    Returns table name, row count, quality score,
    AI description and tags for every table.
    """
    valid_order = ["quality_score", "row_count", "table_name", "column_count"]
    if order_by not in valid_order:
        order_by = "quality_score"

    result = db.execute(text(f"""
        SELECT
            table_name,
            row_count,
            column_count,
            quality_score,
            quality_grade,
            ai_description,
            ai_tags,
            last_crawled_at
        FROM catalog_tables
        ORDER BY {order_by} DESC
        LIMIT :limit
    """), {"limit": limit})

    rows = result.fetchall()
    return {
        "total"  : len(rows),
        "tables" : [dict(row._mapping) for row in rows]
    }


@router.get("/top/quality")
def get_top_quality_tables(
    db: Session = Depends(get_db),
    limit: Optional[int] = 5
):
    """
    Get top tables by quality score.
    Shows the best quality tables in the catalog.
    """
    result = db.execute(text("""
        SELECT
            table_name,
            quality_score,
            quality_grade,
            row_count,
            ai_description
        FROM catalog_tables
        ORDER BY quality_score DESC
        LIMIT :limit
    """), {"limit": limit})

    rows = result.fetchall()
    return {
        "top_tables": [dict(row._mapping) for row in rows]
    }


@router.get("/stats")
def get_catalog_stats(db: Session = Depends(get_db)):
    """
    Get overall catalog statistics.
    Returns summary numbers for the entire catalog.
    """
    result = db.execute(text("""
        SELECT
            COUNT(*)                                    AS total_tables,
            SUM(row_count)                              AS total_rows,
            SUM(column_count)                           AS total_columns,
            ROUND(AVG(quality_score)::numeric, 2)       AS avg_quality,
            COUNT(CASE WHEN quality_grade = 'A' THEN 1 END) AS grade_a_tables,
            COUNT(CASE WHEN ai_description IS NOT NULL THEN 1 END) AS described_tables
        FROM catalog_tables
    """))

    row = result.fetchone()
    return dict(row._mapping)


@router.get("/{table_name}")
def get_table(
    table_name: str,
    db: Session = Depends(get_db)
):
    """
    Get full details for a specific table.
    Returns metadata, quality scores and AI description.
    """
    result = db.execute(text("""
        SELECT
            table_name,
            row_count,
            column_count,
            table_size_bytes,
            primary_keys,
            foreign_key_count,
            quality_score,
            quality_grade,
            completeness,
            uniqueness,
            freshness,
            validity,
            ai_description,
            ai_tags,
            last_crawled_at
        FROM catalog_tables
        WHERE table_name = :table_name
    """), {"table_name": table_name})

    row = result.fetchone()
    if not row:
        raise HTTPException(
            status_code = 404,
            detail      = f"Table '{table_name}' not found in catalog"
        )

    return dict(row._mapping)


@router.get("/{table_name}/columns")
def get_table_columns(
    table_name: str,
    db: Session = Depends(get_db)
):
    """
    Get all columns for a specific table.
    Returns column names, types, null % and AI descriptions.
    """
    # First check table exists
    table = db.execute(text("""
        SELECT table_name FROM catalog_tables
        WHERE table_name = :table_name
    """), {"table_name": table_name}).fetchone()

    if not table:
        raise HTTPException(
            status_code = 404,
            detail      = f"Table '{table_name}' not found"
        )

    result = db.execute(text("""
        SELECT
            column_name,
            clean_type,
            raw_type,
            is_nullable,
            is_primary_key,
            is_foreign_key,
            references_table,
            references_column,
            null_count,
            null_pct,
            ai_description
        FROM catalog_columns
        WHERE table_name = :table_name
        ORDER BY column_name
    """), {"table_name": table_name})

    rows = result.fetchall()
    return {
        "table_name"  : table_name,
        "total_columns": len(rows),
        "columns"     : [dict(row._mapping) for row in rows]
    }


@router.get("/{table_name}/relationships")
def get_table_relationships(
    table_name: str,
    db: Session = Depends(get_db)
):
    """
    Get all foreign key relationships for a table.
    Shows which tables this table connects to.
    """
    result = db.execute(text("""
        SELECT
            source_table,
            source_column,
            target_table,
            target_column,
            relationship_type
        FROM catalog_relationships
        WHERE source_table = :table_name
           OR target_table = :table_name
    """), {"table_name": table_name})

    rows = result.fetchall()
    return {
        "table_name"    : table_name,
        "relationships" : [dict(row._mapping) for row in rows]
    }