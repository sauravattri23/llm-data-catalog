"""
=============================================================
  lineage_tracker.py
  Builds a complete data lineage graph in Neo4j.
  Creates nodes for database, tables, columns and
  relationships between them based on foreign keys.
=============================================================
"""

import os
from datetime import datetime
from neo4j import GraphDatabase
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────
NEO4J_URI      = os.getenv("NEO4J_URI",  "bolt://localhost:7687")
NEO4J_USER     = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASS     = os.getenv("NEO4J_PASS", "neo4j_pass")
DATABASE_URL   = os.getenv("DATABASE_URL",
    "postgresql://catalog_user:catalog_pass@localhost:5432/ecommerce_db")
DATABASE_NAME  = "ecommerce_db"


# ─────────────────────────────────────────────
#  NEO4J HELPER — run queries
# ─────────────────────────────────────────────
class Neo4jConnection:
    """Simple wrapper to connect and run Neo4j queries."""

    def __init__(self):
        self.driver = GraphDatabase.driver(
            NEO4J_URI,
            auth=(NEO4J_USER, NEO4J_PASS)
        )

    def run(self, query: str, params: dict = {}):
        with self.driver.session() as session:
            return session.run(query, params).data()

    def close(self):
        self.driver.close()


# ─────────────────────────────────────────────
#  POSTGRES HELPER — read catalog data
# ─────────────────────────────────────────────
class CatalogReader:
    """Reads metadata from PostgreSQL catalog tables."""

    def __init__(self):
        self.engine = create_engine(DATABASE_URL, echo=False)

    def get_all_tables(self) -> list:
        """Gets all tables with metadata from catalog_tables."""
        with self.engine.connect() as conn:
            result = conn.execute(text("""
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
                ORDER BY table_name
            """))
            return [dict(row._mapping) for row in result]

    def get_all_columns(self) -> list:
        """Gets all columns with metadata from catalog_columns."""
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT
                    table_name,
                    column_name,
                    clean_type,
                    is_nullable,
                    is_primary_key,
                    is_foreign_key,
                    references_table,
                    references_column,
                    null_pct,
                    ai_description
                FROM catalog_columns
                ORDER BY table_name, column_name
            """))
            return [dict(row._mapping) for row in result]

    def get_all_relationships(self) -> list:
        """Gets all foreign key relationships."""
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT
                    source_table,
                    source_column,
                    target_table,
                    target_column,
                    relationship_type
                FROM catalog_relationships
                ORDER BY source_table
            """))
            return [dict(row._mapping) for row in result]


# ─────────────────────────────────────────────
#  LINEAGE TRACKER — main class
# ─────────────────────────────────────────────
class LineageTracker:
    """
    Main class — reads metadata from PostgreSQL catalog
    and builds a complete lineage graph in Neo4j.

    Graph structure:
    (Database) -[CONTAINS]-> (Table) -[HAS_COLUMN]-> (Column)
    (Column)   -[REFERENCES]-> (Column)   (foreign keys)
    (Table)    -[RELATED_TO]-> (Table)    (table relationships)
    """

    def __init__(self):
        self.neo4j   = Neo4jConnection()
        self.catalog = CatalogReader()

    def clear_graph(self):
        """
        Clears all existing nodes and relationships.
        Run this before rebuilding to start fresh.
        """
        print("  🗑️  Clearing existing graph...")
        self.neo4j.run("MATCH (n) DETACH DELETE n")
        print("  ✅ Graph cleared\n")

    def create_constraints(self):
        """
        Creates uniqueness constraints in Neo4j.
        Like PRIMARY KEY in PostgreSQL — ensures no duplicates.
        """
        print("  🔒 Creating constraints...")

        constraints = [
            # Each database name must be unique
            """CREATE CONSTRAINT database_name IF NOT EXISTS
               FOR (d:Database) REQUIRE d.name IS UNIQUE""",

            # Each table has unique name within its database
            """CREATE CONSTRAINT table_name IF NOT EXISTS
               FOR (t:Table) REQUIRE t.full_name IS UNIQUE""",

            # Each column has unique identifier
            """CREATE CONSTRAINT column_name IF NOT EXISTS
               FOR (c:Column) REQUIRE c.full_name IS UNIQUE""",
        ]

        for constraint in constraints:
            try:
                self.neo4j.run(constraint)
            except Exception:
                pass  # Constraint already exists — that's fine

        print("  ✅ Constraints created\n")

    def create_database_node(self):
        """
        Creates the root Database node.
        Everything connects to this node.
        """
        print(f"  📦 Creating database node: {DATABASE_NAME}")

        self.neo4j.run("""
            MERGE (d:Database {name: $name})
            SET d.created_at = $created_at,
                d.type       = 'PostgreSQL'
            RETURN d
        """, {
            "name"      : DATABASE_NAME,
            "created_at": datetime.now().isoformat()
        })

        print(f"  ✅ Database node created\n")

    def create_table_nodes(self, tables: list):
        """
        Creates one Table node per table in catalog.
        Each node stores all metadata about that table.
        """
        print(f"  📋 Creating {len(tables)} table nodes...")

        for table in tables:
            table_name = table["table_name"]
            full_name  = f"{DATABASE_NAME}.{table_name}"

            # Create table node
            self.neo4j.run("""
                MERGE (t:Table {full_name: $full_name})
                SET t.name          = $name,
                    t.database      = $database,
                    t.row_count     = $row_count,
                    t.column_count  = $column_count,
                    t.quality_score = $quality_score,
                    t.quality_grade = $quality_grade,
                    t.description   = $description,
                    t.tags          = $tags
                RETURN t
            """, {
                "full_name"    : full_name,
                "name"         : table_name,
                "database"     : DATABASE_NAME,
                "row_count"    : table["row_count"] or 0,
                "column_count" : table["column_count"] or 0,
                "quality_score": table["quality_score"] or 0,
                "quality_grade": table["quality_grade"] or "N/A",
                "description"  : table["ai_description"] or "",
                "tags"         : table["ai_tags"] or "",
            })

            # Connect table to database
            self.neo4j.run("""
                MATCH (d:Database {name: $db_name})
                MATCH (t:Table    {full_name: $full_name})
                MERGE (d)-[:CONTAINS]->(t)
            """, {
                "db_name"  : DATABASE_NAME,
                "full_name": full_name
            })

        print(f"  ✅ {len(tables)} table nodes created\n")

    def create_column_nodes(self, columns: list):
        """
        Creates one Column node per column in catalog.
        Connects each column to its parent table.
        """
        print(f"  📝 Creating {len(columns)} column nodes...")

        for col in columns:
            table_name  = col["table_name"]
            column_name = col["column_name"]
            full_name   = f"{DATABASE_NAME}.{table_name}.{column_name}"
            table_full  = f"{DATABASE_NAME}.{table_name}"

            # Create column node
            self.neo4j.run("""
                MERGE (c:Column {full_name: $full_name})
                SET c.name          = $name,
                    c.table_name    = $table_name,
                    c.database      = $database,
                    c.data_type     = $data_type,
                    c.is_nullable   = $is_nullable,
                    c.is_primary_key= $is_primary_key,
                    c.is_foreign_key= $is_foreign_key,
                    c.null_pct      = $null_pct,
                    c.description   = $description
                RETURN c
            """, {
                "full_name"     : full_name,
                "name"          : column_name,
                "table_name"    : table_name,
                "database"      : DATABASE_NAME,
                "data_type"     : col["clean_type"] or "unknown",
                "is_nullable"   : col["is_nullable"] or "True",
                "is_primary_key": col["is_primary_key"] or "False",
                "is_foreign_key": col["is_foreign_key"] or "False",
                "null_pct"      : col["null_pct"] or 0,
                "description"   : col["ai_description"] or "",
            })

            # Connect column to its table
            self.neo4j.run("""
                MATCH (t:Table  {full_name: $table_full})
                MATCH (c:Column {full_name: $col_full})
                MERGE (t)-[:HAS_COLUMN]->(c)
            """, {
                "table_full": table_full,
                "col_full"  : full_name
            })

        print(f"  ✅ {len(columns)} column nodes created\n")

    def create_relationships(self, relationships: list):
        """
        Creates REFERENCES relationships between columns
        based on foreign keys.
        Also creates RELATED_TO between tables.

        Example:
        orders.user_id -[REFERENCES]-> users.user_id
        orders         -[RELATED_TO]-> users
        """
        print(f"  🔗 Creating {len(relationships)} relationships...")

        for rel in relationships:
            source_table  = rel["source_table"]
            source_column = rel["source_column"]
            target_table  = rel["target_table"]
            target_column = rel["target_column"]

            if not all([source_table, source_column,
                        target_table, target_column]):
                continue

            source_col_full  = f"{DATABASE_NAME}.{source_table}.{source_column}"
            target_col_full  = f"{DATABASE_NAME}.{target_table}.{target_column}"
            source_table_full= f"{DATABASE_NAME}.{source_table}"
            target_table_full= f"{DATABASE_NAME}.{target_table}"

            # Column → Column relationship (REFERENCES)
            self.neo4j.run("""
                MATCH (src:Column {full_name: $source})
                MATCH (tgt:Column {full_name: $target})
                MERGE (src)-[r:REFERENCES]->(tgt)
                SET r.type = 'foreign_key'
                RETURN r
            """, {
                "source": source_col_full,
                "target": target_col_full
            })

            # Table → Table relationship (RELATED_TO)
            self.neo4j.run("""
                MATCH (src:Table {full_name: $source})
                MATCH (tgt:Table {full_name: $target})
                MERGE (src)-[r:RELATED_TO]->(tgt)
                SET r.via         = $via,
                    r.join_column = $join
                RETURN r
            """, {
                "source": source_table_full,
                "target": target_table_full,
                "via"   : f"{source_column} → {target_column}",
                "join"  : source_column
            })

        print(f"  ✅ {len(relationships)} relationships created\n")

    def get_graph_stats(self) -> dict:
        """Returns counts of all nodes and relationships in graph."""

        nodes = self.neo4j.run("""
            MATCH (n)
            RETURN labels(n)[0] as type, COUNT(n) as count
            ORDER BY count DESC
        """)

        rels = self.neo4j.run("""
            MATCH ()-[r]->()
            RETURN type(r) as type, COUNT(r) as count
            ORDER BY count DESC
        """)

        return {"nodes": nodes, "relationships": rels}

    def run(self):
        """
        Main method — builds complete lineage graph.

        Steps:
        1. Clear existing graph
        2. Create constraints
        3. Create database node
        4. Create table nodes
        5. Create column nodes
        6. Create relationships
        7. Print summary
        """
        print("\n" + "="*60)
        print("  🔗 LLM Data Catalog — Lineage Tracker (Phase 4)")
        print("="*60 + "\n")

        # Read all metadata from PostgreSQL
        print("  📖 Reading metadata from catalog database...")
        tables        = self.catalog.get_all_tables()
        columns       = self.catalog.get_all_columns()
        relationships = self.catalog.get_all_relationships()

        print(f"     → {len(tables)} tables")
        print(f"     → {len(columns)} columns")
        print(f"     → {len(relationships)} relationships\n")

        # Build Neo4j graph
        self.clear_graph()
        self.create_constraints()
        self.create_database_node()
        self.create_table_nodes(tables)
        self.create_column_nodes(columns)
        self.create_relationships(relationships)

        # Get final stats
        stats = self.get_graph_stats()
        self._print_summary(stats, tables, relationships)

        self.neo4j.close()

    def _print_summary(self, stats: dict,
                        tables: list, relationships: list):
        """Prints beautiful summary of lineage graph."""

        print("="*60)
        print("  ✅ LINEAGE GRAPH COMPLETE!")
        print("="*60)

        print(f"\n  📊 Graph Statistics:")
        print(f"  {'─'*40}")
        for node in stats["nodes"]:
            print(f"  {node['type']:<15} nodes: {node['count']}")

        print(f"\n  🔗 Relationships:")
        print(f"  {'─'*40}")
        for rel in stats["relationships"]:
            print(f"  {rel['type']:<20} count: {rel['count']}")

        print(f"\n  📋 Table Relationships Map:")
        print(f"  {'─'*40}")

        # Group relationships by source table
        rel_map = {}
        for rel in relationships:
            src = rel["source_table"]
            tgt = rel["target_table"]
            col = rel["source_column"]
            if src not in rel_map:
                rel_map[src] = []
            rel_map[src].append(f"{tgt} (via {col})")

        for table, targets in rel_map.items():
            print(f"  {table}")
            for t in targets:
                print(f"    └──► {t}")

        print(f"\n  🌐 View your lineage graph:")
        print(f"     Open: http://localhost:7474")
        print(f"     Run:  MATCH (n) RETURN n LIMIT 100")
        print(f"\n  🚀 Ready for Phase 5 — Airflow Orchestration!\n")


# ─────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    tracker = LineageTracker()
    tracker.run()