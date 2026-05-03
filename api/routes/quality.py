"""
=============================================================
  routes/quality.py
  Data quality endpoints.
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


@router.get("/summary")
def get_quality_summary(db: Session = Depends(get_db)):
    """
    Get quality summary for all tables.
    Returns scores sorted from best to worst.
    """
    result = db.execute(text("""
        SELECT
            table_name,
            quality_score,
            quality_grade,
            completeness,
            uniqueness,
            freshness,
            validity,
            row_count
        FROM catalog_tables
        ORDER BY quality_score DESC
    """))

    rows = result.fetchall()
    data = [dict(row._mapping) for row in rows]

    # Calculate grade distribution
    grades = {"A": 0, "B": 0, "C": 0, "D": 0, "F": 0}
    for row in data:
        grade = row.get("quality_grade", "F")
        if grade in grades:
            grades[grade] += 1

    avg_score = sum(r["quality_score"] for r in data if r["quality_score"]) / len(data) if data else 0

    return {
        "total_tables"      : len(data),
        "average_score"     : round(avg_score, 2),
        "grade_distribution": grades,
        "tables"            : data
    }


@router.get("/alerts")
def get_quality_alerts(
    db: Session = Depends(get_db),
    threshold: Optional[float] = 80.0
):
    """
    Get tables with quality score below threshold.
    Default threshold is 80. These tables need attention.
    """
    result = db.execute(text("""
        SELECT
            table_name,
            quality_score,
            quality_grade,
            completeness,
            uniqueness,
            freshness,
            validity,
            row_count
        FROM catalog_tables
        WHERE quality_score < :threshold
        ORDER BY quality_score ASC
    """), {"threshold": threshold})

    rows = result.fetchall()
    data = [dict(row._mapping) for row in rows]

    return {
        "threshold"     : threshold,
        "alerts_count"  : len(data),
        "message"       : f"{len(data)} tables below {threshold} quality score",
        "tables"        : data
    }


@router.get("/columns/nulls")
def get_high_null_columns(
    db: Session = Depends(get_db),
    min_null_pct: Optional[float] = 10.0
):
    """
    Get columns with high null percentages.
    Helps identify data completeness issues.
    """
    result = db.execute(text("""
        SELECT
            table_name,
            column_name,
            clean_type,
            null_pct,
            null_count
        FROM catalog_columns
        WHERE null_pct >= :min_null_pct
        ORDER BY null_pct DESC
    """), {"min_null_pct": min_null_pct})

    rows = result.fetchall()
    return {
        "min_null_pct": min_null_pct,
        "total"       : len(rows),
        "columns"     : [dict(row._mapping) for row in rows]
    }


@router.get("/{table_name}")
def get_table_quality(
    table_name: str,
    db: Session = Depends(get_db)
):
    """
    Get detailed quality report for a specific table.
    Shows all 4 quality dimensions with scores.
    """
    result = db.execute(text("""
        SELECT
            table_name,
            quality_score,
            quality_grade,
            completeness,
            uniqueness,
            freshness,
            validity,
            row_count,
            column_count,
            last_crawled_at
        FROM catalog_tables
        WHERE table_name = :table_name
    """), {"table_name": table_name})

    row = result.fetchone()
    if not row:
        raise HTTPException(
            status_code = 404,
            detail      = f"Table '{table_name}' not found"
        )

    data = dict(row._mapping)

    # Get column null percentages
    col_result = db.execute(text("""
        SELECT column_name, null_pct, clean_type
        FROM catalog_columns
        WHERE table_name = :table_name
        ORDER BY null_pct DESC NULLS LAST
    """), {"table_name": table_name})

    columns = [dict(r._mapping) for r in col_result.fetchall()]

    return {
        "table_name"  : data["table_name"],
        "overall"     : {
            "score": data["quality_score"],
            "grade": data["quality_grade"],
        },
        "dimensions"  : {
            "completeness": data["completeness"],
            "uniqueness"  : data["uniqueness"],
            "freshness"   : data["freshness"],
            "validity"    : data["validity"],
        },
        "metadata"    : {
            "row_count"     : data["row_count"],
            "column_count"  : data["column_count"],
            "last_crawled"  : str(data["last_crawled_at"]),
        },
        "column_nulls": columns
    }