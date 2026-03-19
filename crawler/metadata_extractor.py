"""
=============================================================
  metadata_extractor.py
  Main crawler — connects to PostgreSQL, reads all tables,
  extracts schema + statistics + quality scores,
  saves everything to a catalog database.
==============================================================
"""

import json
from datetime import datetime
from sqlalchemy import create_engine, inspect, text, Column, Integer, String, Float, Text, DateTime, JSON
from sqlalchemy.orm import declarative_base, sessionmaker
import os
from dotenv import load_dotenv

from schema_parser import parse_column, parse_foreign_key, parse_table_summary
from quality_profiler import profile_table

load_dotenv()

# ─────────────────────────────────────────────
#  DATABASE CONNECTIONS
# ─────────────────────────────────────────────

# Source DB — your fake ecommerce database (what we SCAN)
SOURCE_DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://catalog_user:catalog_pass@localhost:5432/ecommerce_db"
)

# Catalog DB — where we SAVE all metadata (same DB, different tables)
CATALOG_DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://catalog_user:catalog_pass@localhost:5432/ecommerce_db"
)

# ─────────────────────────────────────────────
#  CATALOG TABLES (where metadata gets stored)
# ─────────────────────────────────────────────
CatalogBase = declarative_base()


class CatalogTable(CatalogBase):
    """Stores metadata for each discovered table"""
    __tablename__ = "catalog_tables"

    id               = Column(Integer, primary_key=True, autoincrement=True)
    table_name       = Column(String(100), unique=True, nullable=False)
    column_count     = Column(Integer)
    row_count        = Column(Integer)
    table_size_bytes = Column(Integer)
    primary_keys     = Column(String(200))
    foreign_key_count= Column(Integer)
    schema_name      = Column(String(50), default="public")

    # Quality scores
    quality_score    = Column(Float)
    quality_grade    = Column(String(2))
    completeness     = Column(Float)
    uniqueness       = Column(Float)
    freshness        = Column(Float)
    validity         = Column(Float)

    # AI fields (filled in Phase 3)
    ai_description   = Column(Text, nullable=True)
    ai_tags          = Column(String(500), nullable=True)

    # Timestamps
    last_crawled_at  = Column(DateTime, default=datetime.utcnow)
    created_at       = Column(DateTime, default=datetime.utcnow)


class CatalogColumn(CatalogBase):
    """Stores metadata for each column in each table"""
    __tablename__ = "catalog_columns"

    id               = Column(Integer, primary_key=True, autoincrement=True)
    table_name       = Column(String(100), nullable=False)
    column_name      = Column(String(100), nullable=False)
    clean_type       = Column(String(50))
    raw_type         = Column(String(100))
    max_length       = Column(Integer, nullable=True)
    is_nullable      = Column(String(5))
    is_primary_key   = Column(String(5))
    is_foreign_key   = Column(String(5))
    references_table = Column(String(100), nullable=True)
    references_column= Column(String(100), nullable=True)
    null_count       = Column(Integer, nullable=True)
    null_pct         = Column(Float, nullable=True)

    # AI fields (filled in Phase 3)
    ai_description   = Column(Text, nullable=True)

    last_crawled_at  = Column(DateTime, default=datetime.utcnow)


class CatalogRelationship(CatalogBase):
    """Stores foreign key relationships between tables"""
    __tablename__ = "catalog_relationships"

    id                 = Column(Integer, primary_key=True, autoincrement=True)
    source_table       = Column(String(100), nullable=False)
    source_column      = Column(String(100), nullable=False)
    target_table       = Column(String(100), nullable=False)
    target_column      = Column(String(100), nullable=False)
    relationship_type  = Column(String(20), default="many_to_one")
    last_crawled_at    = Column(DateTime, default=datetime.utcnow)


class CrawlLog(CatalogBase):
    """Logs every crawl run for monitoring"""
    __tablename__ = "crawl_logs"

    id              = Column(Integer, primary_key=True, autoincrement=True)
    started_at      = Column(DateTime, default=datetime.utcnow)
    finished_at     = Column(DateTime, nullable=True)
    tables_crawled  = Column(Integer, default=0)
    columns_crawled = Column(Integer, default=0)
    status          = Column(String(20), default="running")
    error_message   = Column(Text, nullable=True)


# ─────────────────────────────────────────────
#  HELPER FUNCTIONS
# ─────────────────────────────────────────────

def get_table_size(engine, table_name: str) -> int:
    """Returns table size in bytes"""
    query = f"SELECT pg_total_relation_size('{table_name}')"
    with engine.connect() as conn:
        return conn.execute(text(query)).scalar() or 0


def get_row_count(engine, table_name: str) -> int:
    """Returns exact row count for a table"""
    with engine.connect() as conn:
        return conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar() or 0


def get_column_stats(engine, table_name: str, column_name: str,
                     clean_type: str, total_rows: int) -> dict:
    """
    Gets basic statistics for a single column:
    - null count and percentage
    - distinct value count
    - most common value (for string columns)
    - min/max (for numeric columns)
    """
    stats = {}

    if total_rows == 0:
        return {"null_count": 0, "null_pct": 0}

    # Null count
    with engine.connect() as conn:
        null_count = conn.execute(
            text(f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} IS NULL")
        ).scalar()

    stats["null_count"]  = null_count
    stats["null_pct"]    = round((null_count / total_rows) * 100, 2)
    stats["filled_pct"]  = round(100 - stats["null_pct"], 2)

    # Distinct count
    with engine.connect() as conn:
        distinct = conn.execute(
            text(f"SELECT COUNT(DISTINCT {column_name}) FROM {table_name}")
        ).scalar()
    stats["distinct_count"] = distinct

    # Min/Max for numeric types
    if clean_type in ["integer", "decimal", "float"] and null_count < total_rows:
        with engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT MIN({column_name}), MAX({column_name}), AVG({column_name}) FROM {table_name}")
            ).fetchone()
        if result:
            stats["min_value"] = str(result[0]) if result[0] is not None else None
            stats["max_value"] = str(result[1]) if result[1] is not None else None
            stats["avg_value"] = str(round(float(result[2]), 2)) if result[2] is not None else None

    # Most common value for string columns
    if clean_type in ["string", "text"] and null_count < total_rows:
        with engine.connect() as conn:
            result = conn.execute(
                text(f"""
                    SELECT {column_name}, COUNT(*) as freq
                    FROM {table_name}
                    WHERE {column_name} IS NOT NULL
                    GROUP BY {column_name}
                    ORDER BY freq DESC
                    LIMIT 3
                """)
            ).fetchall()
        if result:
            stats["top_values"] = [str(r[0]) for r in result]

    return stats


# ─────────────────────────────────────────────
#  MAIN CRAWLER CLASS
# ─────────────────────────────────────────────

class MetadataExtractor:
    """
    Main crawler class.
    Connects to source DB, reads all tables,
    extracts metadata and saves to catalog DB.
    """

    def __init__(self):
        self.source_engine  = create_engine(SOURCE_DB_URL, echo=False)
        self.catalog_engine = create_engine(CATALOG_DB_URL, echo=False)
        CatalogSession      = sessionmaker(bind=self.catalog_engine)
        self.session        = CatalogSession()
        self.inspector      = inspect(self.source_engine)

    def setup_catalog_tables(self):
        """Creates catalog tables if they don't exist"""
        print("  📦 Setting up catalog tables...")
        CatalogBase.metadata.create_all(self.catalog_engine)
        print("  ✅ Catalog tables ready (catalog_tables, catalog_columns, catalog_relationships, crawl_logs)")

    def get_all_tables(self) -> list:
        """Returns list of all table names in source database"""
        all_tables = self.inspector.get_table_names(schema="public")
        # Exclude our own catalog tables
        catalog_table_names = ["catalog_tables", "catalog_columns",
                                "catalog_relationships", "crawl_logs"]
        return [t for t in all_tables if t not in catalog_table_names]

    def extract_table_metadata(self, table_name: str) -> dict:
        """
        Extracts complete metadata for one table:
        - Column names, types, constraints
        - Foreign key relationships
        - Row count and size
        - Column-level statistics
        """
        print(f"\n  🔍 Crawling: {table_name}")

        # Get raw columns from SQLAlchemy inspector
        raw_columns = self.inspector.get_columns(table_name, schema="public")
        raw_fks     = self.inspector.get_foreign_keys(table_name, schema="public")
        pk_info     = self.inspector.get_pk_constraint(table_name, schema="public")

        # Parse columns and foreign keys
        columns      = [parse_column(col) for col in raw_columns]
        foreign_keys = [parse_foreign_key(fk) for fk in raw_fks]
        pk_cols      = pk_info.get("constrained_columns", [])

        # Mark primary key columns
        for col in columns:
            if col["column_name"] in pk_cols:
                col["is_primary_key"] = True

        # Build full table summary
        table_summary = parse_table_summary(table_name, columns, foreign_keys)

        # Get row count and size
        row_count        = get_row_count(self.source_engine, table_name)
        table_size_bytes = get_table_size(self.source_engine, table_name)

        print(f"     📋 {len(columns)} columns | {row_count:,} rows | {table_size_bytes/1024:.1f} KB")

        # Get column statistics
        print(f"     📈 Collecting column statistics...")
        for col in columns:
            stats = get_column_stats(
                self.source_engine, table_name,
                col["column_name"], col["clean_type"], row_count
            )
            col.update(stats)

        # Run quality profiling
        quality = profile_table(
            self.source_engine, table_name, columns, pk_cols
        )

        return {
            "table_name"      : table_name,
            "columns"         : columns,
            "foreign_keys"    : foreign_keys,
            "primary_keys"    : pk_cols,
            "row_count"       : row_count,
            "table_size_bytes": table_size_bytes,
            "column_count"    : len(columns),
            "quality"         : quality,
        }

    def save_to_catalog(self, metadata: dict):
        """Saves extracted metadata to catalog tables"""
        table_name = metadata["table_name"]

        # ── Save/Update catalog_tables ──
        existing = self.session.query(CatalogTable).filter_by(
            table_name=table_name).first()

        quality = metadata["quality"]

        if existing:
            # Update existing record
            existing.column_count      = metadata["column_count"]
            existing.row_count         = metadata["row_count"]
            existing.table_size_bytes  = metadata["table_size_bytes"]
            existing.primary_keys      = ", ".join(metadata["primary_keys"])
            existing.foreign_key_count = len(metadata["foreign_keys"])
            existing.quality_score     = quality["overall_score"]
            existing.quality_grade     = quality["grade"]
            existing.completeness      = quality["completeness"]["score"]
            existing.uniqueness        = quality["uniqueness"]["score"]
            existing.freshness         = quality["freshness"]["score"]
            existing.validity          = quality["validity"]["score"]
            existing.last_crawled_at   = datetime.utcnow()
        else:
            # Insert new record
            catalog_entry = CatalogTable(
                table_name       = table_name,
                column_count     = metadata["column_count"],
                row_count        = metadata["row_count"],
                table_size_bytes = metadata["table_size_bytes"],
                primary_keys     = ", ".join(metadata["primary_keys"]),
                foreign_key_count= len(metadata["foreign_keys"]),
                quality_score    = quality["overall_score"],
                quality_grade    = quality["grade"],
                completeness     = quality["completeness"]["score"],
                uniqueness       = quality["uniqueness"]["score"],
                freshness        = quality["freshness"]["score"],
                validity         = quality["validity"]["score"],
            )
            self.session.add(catalog_entry)

        # ── Save catalog_columns ──
        # Delete old columns first (clean update)
        self.session.query(CatalogColumn).filter_by(table_name=table_name).delete()

        for col in metadata["columns"]:
            fk_ref = col.get("references") or {}
            catalog_col = CatalogColumn(
                table_name       = table_name,
                column_name      = col["column_name"],
                clean_type       = col["clean_type"],
                raw_type         = col["raw_type"],
                max_length       = col.get("max_length"),
                is_nullable      = str(col["is_nullable"]),
                is_primary_key   = str(col["is_primary_key"]),
                is_foreign_key   = str(col.get("is_foreign_key", False)),
                references_table = fk_ref.get("references_table"),
                references_column= fk_ref.get("references_column"),
                null_count       = col.get("null_count"),
                null_pct         = col.get("null_pct"),
            )
            self.session.add(catalog_col)

        # ── Save catalog_relationships ──
        self.session.query(CatalogRelationship).filter_by(
            source_table=table_name).delete()

        for fk in metadata["foreign_keys"]:
            if fk["column"] and fk["references_table"]:
                rel = CatalogRelationship(
                    source_table  = table_name,
                    source_column = fk["column"],
                    target_table  = fk["references_table"],
                    target_column = fk["references_column"],
                )
                self.session.add(rel)

        self.session.commit()

    def run(self):
        """
        Main method — runs the complete crawl:
        1. Setup catalog tables
        2. Get all source tables
        3. Extract metadata for each table
        4. Save to catalog
        5. Print summary
        """
        print("\n" + "="*60)
        print("  🕷️  LLM Data Catalog — Metadata Crawler")
        print("="*60)

        # Setup catalog tables
        self.setup_catalog_tables()

        # Start crawl log
        crawl_log = CrawlLog(started_at=datetime.utcnow(), status="running")
        self.session.add(crawl_log)
        self.session.commit()

        # Get all tables
        tables = self.get_all_tables()
        print(f"\n  📋 Found {len(tables)} tables to crawl:")
        for t in tables:
            print(f"     → {t}")

        # Crawl each table
        print(f"\n  🚀 Starting crawl...\n")
        results         = []
        total_columns   = 0
        errors          = []

        for table_name in tables:
            try:
                metadata = self.extract_table_metadata(table_name)
                self.save_to_catalog(metadata)
                results.append(metadata)
                total_columns += metadata["column_count"]
                print(f"     ✅ Saved to catalog — Score: {metadata['quality']['overall_score']}/100 ({metadata['quality']['grade']})")
            except Exception as e:
                errors.append(f"{table_name}: {str(e)}")
                print(f"     ❌ Error crawling {table_name}: {e}")

        # Update crawl log
        crawl_log.finished_at    = datetime.utcnow()
        crawl_log.tables_crawled = len(results)
        crawl_log.columns_crawled= total_columns
        crawl_log.status         = "success" if not errors else "partial"
        crawl_log.error_message  = "\n".join(errors) if errors else None
        self.session.commit()

        # Print final summary
        self._print_summary(results, total_columns, errors)

    def _print_summary(self, results: list, total_columns: int, errors: list):
        """Prints a beautiful summary of the crawl"""
        print("\n" + "="*60)
        print("  ✅ CRAWL COMPLETE!")
        print("="*60)
        print(f"\n  📊 Summary:")
        print(f"     Tables crawled:   {len(results)}")
        print(f"     Columns cataloged: {total_columns}")
        print(f"     Errors:           {len(errors)}")

        print(f"\n  📋 Table Quality Scores:")
        print(f"  {'Table':<20} {'Rows':>8} {'Columns':>8} {'Score':>8} {'Grade':>6}")
        print(f"  {'-'*55}")

        sorted_results = sorted(results, key=lambda x: x["quality"]["overall_score"], reverse=True)
        for r in sorted_results:
            print(f"  {r['table_name']:<20} "
                  f"{r['row_count']:>8,} "
                  f"{r['column_count']:>8} "
                  f"{r['quality']['overall_score']:>8.1f} "
                  f"{r['quality']['grade']:>6}")

        if errors:
            print(f"\n  ❌ Errors encountered:")
            for e in errors:
                print(f"     {e}")

        print(f"\n  🎯 Metadata saved to catalog tables:")
        print(f"     → catalog_tables       (table-level metadata)")
        print(f"     → catalog_columns      (column-level metadata)")
        print(f"     → catalog_relationships(foreign key relationships)")
        print(f"     → crawl_logs           (crawl history)")
        print(f"\n  🚀 Ready for Phase 3 — LLM Description Generation!\n")


# ─────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    crawler = MetadataExtractor()
    crawler.run()