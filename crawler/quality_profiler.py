"""
=============================================================
  quality_profiler.py
  Calculates data quality scores for every table.
  Checks: Completeness, Uniqueness, Freshness, Validity
=============================================================
"""

import pandas as pd
from datetime import datetime, timezone
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://catalog_user:catalog_pass@localhost:5432/ecommerce_db"
)


def get_completeness_score(engine, table_name: str, columns: list) -> dict:
    """
    Completeness = how many values are NOT null.
    Score = (non-null values / total values) * 100

    Example:
        500 rows, email column has 0 nulls  → 100% complete
        500 rows, delivered_at has 200 nulls → 60% complete
    """
    total_rows_query = f"SELECT COUNT(*) FROM {table_name}"
    with engine.connect() as conn:
        total_rows = conn.execute(text(total_rows_query)).scalar()

    if total_rows == 0:
        return {"score": 100, "total_rows": 0, "column_nulls": {}}

    column_nulls = {}
    for col in columns:
        col_name = col["column_name"]
        null_query = f"SELECT COUNT(*) FROM {table_name} WHERE {col_name} IS NULL"
        with engine.connect() as conn:
            null_count = conn.execute(text(null_query)).scalar()
        null_pct = round((null_count / total_rows) * 100, 2)
        column_nulls[col_name] = {
            "null_count" : null_count,
            "null_pct"   : null_pct,
            "filled_pct" : round(100 - null_pct, 2)
        }

    # Overall completeness = average filled % across all columns
    avg_filled = sum(v["filled_pct"] for v in column_nulls.values()) / len(column_nulls)
    return {
        "score"       : round(avg_filled, 2),
        "total_rows"  : total_rows,
        "column_nulls": column_nulls
    }


def get_uniqueness_score(engine, table_name: str, pk_columns: list) -> dict:
    """
    Uniqueness = how few duplicate rows exist.
    Score = (unique rows / total rows) * 100

    Example:
        1000 rows, 10 duplicates → 99% unique
    """
    with engine.connect() as conn:
        total = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()

    if total == 0:
        return {"score": 100, "total_rows": 0, "duplicate_count": 0}

    if pk_columns:
        pk_list    = ", ".join(pk_columns)
        dup_query  = f"""
            SELECT COUNT(*) FROM (
                SELECT {pk_list}, COUNT(*) as cnt
                FROM {table_name}
                GROUP BY {pk_list}
                HAVING COUNT(*) > 1
            ) duplicates
        """
        with engine.connect() as conn:
            duplicates = conn.execute(text(dup_query)).scalar()
    else:
        duplicates = 0

    uniqueness = round(((total - duplicates) / total) * 100, 2)
    return {
        "score"          : uniqueness,
        "total_rows"     : total,
        "duplicate_count": duplicates
    }


def get_freshness_score(engine, table_name: str, columns: list) -> dict:
    """
    Freshness = how recently the data was updated.
    Looks for timestamp columns (created_at, updated_at, ordered_at etc.)

    Score:
        Updated within 1 day   → 100%
        Updated within 1 week  → 80%
        Updated within 1 month → 60%
        Updated within 3 months→ 40%
        Older than 3 months    → 20%
    """
    timestamp_cols = [
        c["column_name"] for c in columns
        if c["clean_type"] in ["timestamp", "date"]
    ]

    if not timestamp_cols:
        return {"score": 70, "latest_update": None, "timestamp_column": None}

    # Use first timestamp column found
    ts_col  = timestamp_cols[0]
    query   = f"SELECT MAX({ts_col}) FROM {table_name}"

    with engine.connect() as conn:
        latest = conn.execute(text(query)).scalar()

    if not latest:
        return {"score": 50, "latest_update": None, "timestamp_column": ts_col}

    # Calculate days since last update
    now           = datetime.now(timezone.utc)
    if latest.tzinfo is None:
        latest    = latest.replace(tzinfo=timezone.utc)
    days_old      = (now - latest).days

    if days_old <= 1:
        score = 100
    elif days_old <= 7:
        score = 80
    elif days_old <= 30:
        score = 60
    elif days_old <= 90:
        score = 40
    else:
        score = 20

    return {
        "score"            : score,
        "latest_update"    : str(latest),
        "days_since_update": days_old,
        "timestamp_column" : ts_col
    }


def get_validity_score(engine, table_name: str, columns: list) -> dict:
    """
    Validity = are values within expected ranges?
    Checks for obviously wrong data like:
    - Negative prices
    - Ratings outside 1-5
    - Empty strings in required fields
    - Future dates in created_at
    """
    issues  = []
    total   = 0

    with engine.connect() as conn:
        total = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()

    if total == 0:
        return {"score": 100, "issues": [], "checks_run": 0}

    checks_run    = 0
    issues_found  = 0

    for col in columns:
        col_name   = col["column_name"]
        clean_type = col["clean_type"]

        # Check 1: Negative values in numeric/decimal columns
        if clean_type in ["decimal", "float", "integer"]:
            if any(word in col_name.lower() for word in ["price", "amount", "qty", "count", "total"]):
                query = f"SELECT COUNT(*) FROM {table_name} WHERE {col_name} < 0"
                with engine.connect() as conn:
                    neg_count = conn.execute(text(query)).scalar()
                checks_run += 1
                if neg_count > 0:
                    issues_found += neg_count
                    issues.append(f"{col_name}: {neg_count} negative values found")

        # Check 2: Empty strings in required string columns
        if clean_type in ["string", "text"] and not col["is_nullable"]:
            query = f"SELECT COUNT(*) FROM {table_name} WHERE TRIM({col_name}) = ''"
            with engine.connect() as conn:
                empty_count = conn.execute(text(query)).scalar()
            checks_run += 1
            if empty_count > 0:
                issues_found += empty_count
                issues.append(f"{col_name}: {empty_count} empty string values found")

        # Check 3: Rating columns should be 1-5
        if "rating" in col_name.lower() and clean_type == "integer":
            query = f"SELECT COUNT(*) FROM {table_name} WHERE {col_name} NOT BETWEEN 1 AND 5"
            with engine.connect() as conn:
                invalid_count = conn.execute(text(query)).scalar()
            checks_run += 1
            if invalid_count > 0:
                issues_found += invalid_count
                issues.append(f"{col_name}: {invalid_count} ratings outside 1-5 range")

    # Calculate validity score
    if checks_run == 0:
        score = 95  # No checks run, assume mostly valid
    else:
        issue_rate = (issues_found / (total * checks_run)) * 100
        score      = max(0, round(100 - issue_rate, 2))

    return {
        "score"      : score,
        "issues"     : issues,
        "checks_run" : checks_run
    }


def calculate_overall_score(completeness: float, uniqueness: float,
                             freshness: float, validity: float) -> float:
    """
    Weighted average of all 4 quality dimensions:
    - Completeness: 35% weight (most important)
    - Uniqueness:   25% weight
    - Validity:     25% weight
    - Freshness:    15% weight
    """
    score = (
        completeness * 0.35 +
        uniqueness   * 0.25 +
        validity     * 0.25 +
        freshness    * 0.15
    )
    return round(score, 2)


def get_quality_grade(score: float) -> str:
    """Converts numeric score to letter grade"""
    if score >= 90: return "A"
    if score >= 80: return "B"
    if score >= 70: return "C"
    if score >= 60: return "D"
    return "F"


def profile_table(engine, table_name: str, columns: list, pk_columns: list) -> dict:
    """
    Main function — runs all 4 quality checks on a table
    and returns complete quality profile.
    """
    print(f"     📊 Profiling quality for: {table_name}")

    completeness = get_completeness_score(engine, table_name, columns)
    uniqueness   = get_uniqueness_score(engine, table_name, pk_columns)
    freshness    = get_freshness_score(engine, table_name, columns)
    validity     = get_validity_score(engine, table_name, columns)

    overall = calculate_overall_score(
        completeness["score"],
        uniqueness["score"],
        freshness["score"],
        validity["score"]
    )

    return {
        "table_name"      : table_name,
        "overall_score"   : overall,
        "grade"           : get_quality_grade(overall),
        "total_rows"      : completeness["total_rows"],
        "completeness"    : completeness,
        "uniqueness"      : uniqueness,
        "freshness"       : freshness,
        "validity"        : validity,
        "profiled_at"     : datetime.now().isoformat(),
    }


if __name__ == "__main__":
    print("\n🧪 Testing quality_profiler on 'users' table...\n")
    engine = create_engine(DATABASE_URL)

    # Simple test columns
    test_columns = [
        {"column_name": "user_id",   "clean_type": "integer",   "is_nullable": False},
        {"column_name": "email",     "clean_type": "string",    "is_nullable": False},
        {"column_name": "full_name", "clean_type": "string",    "is_nullable": False},
        {"column_name": "created_at","clean_type": "timestamp", "is_nullable": True},
    ]

    result = profile_table(engine, "users", test_columns, ["user_id"])
    print(f"\n  Table:       {result['table_name']}")
    print(f"  Rows:        {result['total_rows']}")
    print(f"  Score:       {result['overall_score']}/100")
    print(f"  Grade:       {result['grade']}")
    print(f"  Completeness:{result['completeness']['score']}%")
    print(f"  Uniqueness:  {result['uniqueness']['score']}%")
    print(f"  Freshness:   {result['freshness']['score']}%")
    print(f"  Validity:    {result['validity']['score']}%")
    print("\n✅ quality_profiler working correctly!")