"""
=============================================================
  routes/search.py
  Search endpoints — search tables and columns by keyword.
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
def search_catalog(
    q: str,
    db: Session        = Depends(get_db),
    type: Optional[str]= "all",          # all, table, column
    min_quality: Optional[float] = 0.0,
    limit: Optional[int] = 20
):
    """
    Search the entire catalog by keyword.

    Searches across:
    - Table names
    - Table AI descriptions
    - Table tags
    - Column names
    - Column AI descriptions

    Parameters:
    - q: search keyword (required)
    - type: 'all', 'table' or 'column' (default: all)
    - min_quality: minimum quality score filter
    - limit: max results to return
    """
    if not q or len(q.strip()) < 1:
        raise HTTPException(
            status_code = 400,
            detail      = "Search query cannot be empty"
        )

    keyword    = f"%{q.lower()}%"
    results    = {}

    # Search tables
    if type in ["all", "table"]:
        table_result = db.execute(text("""
            SELECT
                table_name,
                row_count,
                quality_score,
                quality_grade,
                ai_description,
                ai_tags,
                'table' AS result_type
            FROM catalog_tables
            WHERE (
                LOWER(table_name)    LIKE :keyword OR
                LOWER(ai_description)LIKE :keyword OR
                LOWER(ai_tags)       LIKE :keyword
            )
            AND quality_score >= :min_quality
            ORDER BY quality_score DESC
            LIMIT :limit
        """), {
            "keyword"    : keyword,
            "min_quality": min_quality,
            "limit"      : limit
        })
        results["tables"] = [dict(r._mapping) for r in table_result.fetchall()]

    # Search columns
    if type in ["all", "column"]:
        col_result = db.execute(text("""
            SELECT
                c.table_name,
                c.column_name,
                c.clean_type,
                c.null_pct,
                c.is_primary_key,
                c.is_foreign_key,
                c.ai_description,
                t.quality_score,
                'column' AS result_type
            FROM catalog_columns c
            JOIN catalog_tables t ON c.table_name = t.table_name
            WHERE (
                LOWER(c.column_name)    LIKE :keyword OR
                LOWER(c.ai_description) LIKE :keyword
            )
            AND t.quality_score >= :min_quality
            ORDER BY t.quality_score DESC
            LIMIT :limit
        """), {
            "keyword"    : keyword,
            "min_quality": min_quality,
            "limit"      : limit
        })
        results["columns"] = [dict(r._mapping) for r in col_result.fetchall()]

    # Build summary
    total = sum(len(v) for v in results.values())

    return {
        "query"  : q,
        "total"  : total,
        "results": results
    }


@router.get("/tables")
def search_tables_only(
    q: str,
    db: Session = Depends(get_db),
    limit: Optional[int] = 10
):
    """
    Search tables only by keyword.
    Faster than full catalog search.
    """
    keyword = f"%{q.lower()}%"

    result = db.execute(text("""
        SELECT
            table_name,
            row_count,
            quality_score,
            quality_grade,
            ai_description,
            ai_tags
        FROM catalog_tables
        WHERE
            LOWER(table_name)     LIKE :keyword OR
            LOWER(ai_description) LIKE :keyword OR
            LOWER(ai_tags)        LIKE :keyword
        ORDER BY quality_score DESC
        LIMIT :limit
    """), {"keyword": keyword, "limit": limit})

    rows = result.fetchall()
    return {
        "query"  : q,
        "total"  : len(rows),
        "tables" : [dict(row._mapping) for row in rows]
    }


@router.get("/columns")
def search_columns_only(
    q: str,
    db: Session = Depends(get_db),
    limit: Optional[int] = 20
):
    """
    Search columns only by keyword.
    """
    keyword = f"%{q.lower()}%"

    result = db.execute(text("""
        SELECT
            c.table_name,
            c.column_name,
            c.clean_type,
            c.null_pct,
            c.ai_description
        FROM catalog_columns c
        WHERE
            LOWER(c.column_name)    LIKE :keyword OR
            LOWER(c.ai_description) LIKE :keyword
        ORDER BY c.table_name, c.column_name
        LIMIT :limit
    """), {"keyword": keyword, "limit": limit})

    rows = result.fetchall()
    return {
        "query"  : q,
        "total"  : len(rows),
        "columns": [dict(row._mapping) for row in rows]
    }