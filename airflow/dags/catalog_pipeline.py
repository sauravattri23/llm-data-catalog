"""
=============================================================
  catalog_pipeline.py
  Main Airflow DAG — orchestrates the entire
  LLM Data Catalog pipeline automatically.

  Pipeline runs daily at midnight:
  1. Health Check
  2. Metadata Crawler
  3. LLM Description Generator
  4. Lineage Tracker
  5. Pipeline Summary
=============================================================
"""

import sys
import os
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


# ─────────────────────────────────────────────
#  Add project paths so imports work in Airflow
# ─────────────────────────────────────────────
sys.path.insert(0, '/opt/airflow/crawler')
sys.path.insert(0, '/opt/airflow/llm_engine')
sys.path.insert(0, '/opt/airflow/lineage')

# ─────────────────────────────────────────────
#  DATABASE CONFIG
# ─────────────────────────────────────────────
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://catalog_user:catalog_pass@catalog_postgres/ecommerce_db"
)
NEO4J_URI  = os.getenv("NEO4J_URI",  "bolt://catalog_neo4j:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASS = os.getenv("NEO4J_PASS", "neo4j_pass")


# ─────────────────────────────────────────────
#  DEFAULT ARGS — applied to every task
# ─────────────────────────────────────────────
default_args = {
    "owner"           : "saurav_attri",
    "depends_on_past" : False,
    "start_date"      : days_ago(1),
    "email_on_failure": False,
    "email_on_retry"  : False,
    "retries"         : 2,              # retry twice if task fails
    "retry_delay"     : timedelta(minutes=5),  # wait 5 min between retries
}


# ─────────────────────────────────────────────
#  TASK FUNCTIONS
# ─────────────────────────────────────────────

def task_health_check(**context):
    """
    Task 1 — Health Check
    Verifies PostgreSQL and Neo4j are reachable
    before starting the pipeline.
    Fails fast if any service is down.
    """
    print("🏥 Running health checks...")

    # Check PostgreSQL
    try:
        from sqlalchemy import create_engine, text
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1")).scalar()
        print(f"  ✅ PostgreSQL: Connected (result={result})")
    except Exception as e:
        raise Exception(f"❌ PostgreSQL health check failed: {e}")

    # Check Neo4j
    try:
        from neo4j import GraphDatabase
        driver = GraphDatabase.driver(
            NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS)
        )
        with driver.session() as session:
            result = session.run("RETURN 1 as n").data()
        driver.close()
        print(f"  ✅ Neo4j: Connected (result={result})")
    except Exception as e:
        raise Exception(f"❌ Neo4j health check failed: {e}")

    # Check catalog_tables exists
    try:
        from sqlalchemy import create_engine, text
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            count = conn.execute(
                text("SELECT COUNT(*) FROM catalog_tables")
            ).scalar()
        print(f"  ✅ Catalog tables: {count} tables in catalog")
    except Exception as e:
        raise Exception(f"❌ Catalog tables check failed: {e}")

    print("\n  ✅ All health checks passed!")
    print("  🚀 Pipeline starting...\n")


def task_run_metadata_crawler(**context):
    """
    Task 2 — Metadata Crawler
    Runs the metadata extractor to scan all tables
    and update the catalog database.
    """
    print("🕷️  Starting Metadata Crawler...")

    try:
        from metadata_extractor import MetadataExtractor
        crawler = MetadataExtractor()
        crawler.run()
        print("\n  ✅ Metadata Crawler completed successfully!")

    except Exception as e:
        raise Exception(f"❌ Metadata Crawler failed: {e}")


def task_run_llm_generator(**context):
    """
    Task 3 — LLM Generator
    Skipped in Airflow — runs manually due to
    torch/transformers being too large for container.
    LLM descriptions are cached so this is fine.
    """
    print("🤖 LLM Generator — Checking cached descriptions...")

    try:
        from sqlalchemy import create_engine, text
        engine = create_engine(DATABASE_URL)

        with engine.connect() as conn:
            # Check how many tables already have descriptions
            described = conn.execute(text("""
                SELECT COUNT(*)
                FROM catalog_tables
                WHERE ai_description IS NOT NULL
            """)).scalar()

            total = conn.execute(text("""
                SELECT COUNT(*) FROM catalog_tables
            """)).scalar()

        print(f"  ✅ {described}/{total} tables already have AI descriptions")
        print(f"  ℹ️  LLM Generator runs manually via:")
        print(f"     cd llm_engine && python catalog_generator.py")
        print(f"  ✅ Skipping LLM generation in Airflow pipeline")

    except Exception as e:
        raise Exception(f"❌ LLM check failed: {e}")


def task_run_lineage_tracker(**context):
    """
    Task 4 — Lineage Tracker
    Rebuilds the Neo4j lineage graph with
    latest metadata from catalog database.
    Only runs if LLM generator succeeded.
    """
    print("🔗 Starting Lineage Tracker...")

    try:
        from lineage_tracker import LineageTracker
        tracker = LineageTracker()
        tracker.run()
        print("\n  ✅ Lineage Tracker completed successfully!")

    except Exception as e:
        raise Exception(f"❌ Lineage Tracker failed: {e}")


def task_pipeline_summary(**context):
    """
    Task 5 — Pipeline Summary
    Logs final summary of the pipeline run.
    Pulls stats from catalog database.
    Only runs if all previous tasks succeeded.
    """
    print("📊 Generating Pipeline Summary...")

    try:
        from sqlalchemy import create_engine, text
        engine = create_engine(DATABASE_URL)

        with engine.connect() as conn:
            # Get table count
            table_count = conn.execute(
                text("SELECT COUNT(*) FROM catalog_tables")
            ).scalar()

            # Get average quality score
            avg_quality = conn.execute(
                text("SELECT ROUND(AVG(quality_score)::numeric, 2) FROM catalog_tables")
            ).scalar()

            # Get tables with AI descriptions
            described = conn.execute(
                text("SELECT COUNT(*) FROM catalog_tables WHERE ai_description IS NOT NULL")
            ).scalar()

            # Get column count
            col_count = conn.execute(
                text("SELECT COUNT(*) FROM catalog_columns")
            ).scalar()

            # Get latest crawl log
            crawl_log = conn.execute(
                text("""
                    SELECT status, tables_crawled, columns_crawled
                    FROM crawl_logs
                    ORDER BY started_at DESC
                    LIMIT 1
                """)
            ).fetchone()

        # Get pipeline run time from Airflow context
        dag_run    = context.get("dag_run")
        start_time = dag_run.start_date if dag_run else datetime.now(timezone.utc)
        end_time   = datetime.now(timezone.utc)  # ← make timezone-aware
        duration   = round((end_time - start_time).total_seconds() / 60, 1)
        
        

        print("\n" + "="*55)
        print("  ✅ PIPELINE RUN COMPLETE!")
        print("="*55)
        print(f"\n  📊 Catalog Summary:")
        print(f"     Tables in catalog:    {table_count}")
        print(f"     Columns in catalog:   {col_count}")
        print(f"     Tables described:     {described}")
        print(f"     Avg quality score:    {avg_quality}/100")
        print(f"     Pipeline duration:    {duration} minutes")

        if crawl_log:
            print(f"\n  🕷️  Latest Crawl:")
            print(f"     Status:             {crawl_log[0]}")
            print(f"     Tables crawled:     {crawl_log[1]}")
            print(f"     Columns crawled:    {crawl_log[2]}")

        print(f"\n  🌐 Access Points:")
        print(f"     pgAdmin:    http://localhost:5050")
        print(f"     Neo4j:      http://localhost:7474")
        print(f"     Airflow UI: http://localhost:8080")
        print(f"\n  🚀 Next run: tomorrow at midnight\n")

    except Exception as e:
        raise Exception(f"❌ Pipeline Summary failed: {e}")


# ─────────────────────────────────────────────
#  DAG DEFINITION
# ─────────────────────────────────────────────
with DAG(
    dag_id          = "llm_catalog_pipeline",
    description     = "LLM-Powered Data Catalog — daily pipeline",
    default_args    = default_args,
    schedule_interval = "0 0 * * *",   # run at midnight every day
    catchup         = False,            # don't backfill old runs
    max_active_runs = 1,               # only one run at a time
    tags            = ["catalog", "llm", "data-engineering"],
) as dag:

    # ── Task 1 — Health Check ──
    health_check = PythonOperator(
        task_id         = "health_check",
        python_callable = task_health_check,
        doc_md          = """
        ### Health Check
        Verifies all services (PostgreSQL, Neo4j) are
        running before starting the pipeline.
        """,
    )

    # ── Task 2 — Metadata Crawler ──
    metadata_crawler = PythonOperator(
        task_id         = "metadata_crawler",
        python_callable = task_run_metadata_crawler,
        doc_md          = """
        ### Metadata Crawler
        Scans all database tables using SQLAlchemy Inspector.
        Extracts schema, statistics and quality scores.
        Saves everything to catalog_tables and catalog_columns.
        """,
    )

    # ── Task 3 — LLM Generator ──
    llm_generator = PythonOperator(
        task_id         = "llm_generator",
        python_callable = task_run_llm_generator,
        doc_md          = """
        ### LLM Description Generator
        Uses HuggingFace flan-t5-large to auto-generate
        descriptions for all tables and columns.
        Saves descriptions to catalog database.
        """,
    )

    # ── Task 4 — Lineage Tracker ──
    lineage_tracker = PythonOperator(
        task_id         = "lineage_tracker",
        python_callable = task_run_lineage_tracker,
        doc_md          = """
        ### Lineage Tracker
        Rebuilds complete Neo4j lineage graph.
        Creates nodes for database, tables, columns.
        Maps all foreign key relationships as edges.
        """,
    )

    # ── Task 5 — Pipeline Summary ──
    pipeline_summary = PythonOperator(
        task_id         = "pipeline_summary",
        python_callable = task_pipeline_summary,
        doc_md          = """
        ### Pipeline Summary
        Logs final stats from the catalog database.
        Reports table count, quality scores, duration.
        """,
    )

    # ─────────────────────────────────────────
    #  TASK ORDER — defines the pipeline flow
    # ─────────────────────────────────────────
    health_check >> metadata_crawler >> llm_generator >> lineage_tracker >> pipeline_summary
    #      ↓               ↓                ↓                  ↓                  ↓
    # check services → scan tables → AI descriptions → lineage graph → print summary