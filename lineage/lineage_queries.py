"""
=============================================================
  lineage_queries.py
  Pre-built queries to explore your lineage graph.
  Run these in Neo4j browser or call from Python.
=============================================================
"""

import os
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

NEO4J_URI  = os.getenv("NEO4J_URI",  "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASS = os.getenv("NEO4J_PASS", "neo4j_pass")


class LineageQueries:
    """
    Pre-built lineage queries.
    Each method answers a specific lineage question.
    """

    def __init__(self):
        self.driver = GraphDatabase.driver(
            NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS)
        )

    def run(self, query: str, params: dict = {}) -> list:
        with self.driver.session() as session:
            return session.run(query, params).data()

    # ─────────────────────────────────────────
    #  QUERY 1 — Find all tables connected to a table
    # ─────────────────────────────────────────
    def get_related_tables(self, table_name: str) -> list:
        """
        Finds all tables directly connected to a given table.

        Example: get_related_tables("orders")
        → users, payments, shipments, order_items
        """
        result = self.run("""
            MATCH (t:Table {name: $table_name})-[r:RELATED_TO]-(related:Table)
            RETURN related.name    AS table,
                   r.via           AS via,
                   related.row_count AS rows,
                   related.quality_score AS quality
            ORDER BY related.name
        """, {"table_name": table_name})

        return result

    # ─────────────────────────────────────────
    #  QUERY 2 — Find upstream tables (where data comes FROM)
    # ─────────────────────────────────────────
    def get_upstream(self, table_name: str) -> list:
        """
        Finds all tables that feed data INTO this table.
        i.e. tables this table has foreign keys TO.

        Example: get_upstream("orders")
        → users (orders.user_id references users.user_id)
        """
        result = self.run("""
            MATCH (src:Table {name: $table_name})-[:HAS_COLUMN]->
                  (col:Column)-[:REFERENCES]->(ref:Column)<-
                  [:HAS_COLUMN]-(tgt:Table)
            RETURN DISTINCT
                   tgt.name        AS upstream_table,
                   col.name        AS from_column,
                   ref.name        AS to_column,
                   tgt.description AS description
            ORDER BY upstream_table
        """, {"table_name": table_name})

        return result

    # ─────────────────────────────────────────
    #  QUERY 3 — Find downstream tables (where data GOES TO)
    # ─────────────────────────────────────────
    def get_downstream(self, table_name: str) -> list:
        """
        Finds all tables that reference THIS table.
        i.e. tables that have foreign keys pointing here.

        Example: get_downstream("users")
        → orders, addresses, reviews, user_events
        """
        result = self.run("""
            MATCH (src:Table {name: $table_name})-[:HAS_COLUMN]->
                  (col:Column)<-[:REFERENCES]-(ref:Column)<-
                  [:HAS_COLUMN]-(tgt:Table)
            RETURN DISTINCT
                   tgt.name        AS downstream_table,
                   ref.name        AS from_column,
                   col.name        AS to_column,
                   tgt.description AS description
            ORDER BY downstream_table
        """, {"table_name": table_name})

        return result

    # ─────────────────────────────────────────
    #  QUERY 4 — Full lineage path between 2 tables
    # ─────────────────────────────────────────
    def get_lineage_path(self, from_table: str, to_table: str) -> list:
        """
        Finds the shortest path between two tables.

        Example: get_lineage_path("user_events", "payments")
        → user_events → users → orders → payments
        """
        result = self.run("""
            MATCH path = shortestPath(
                (start:Table {name: $from_table})-[*]-(end:Table {name: $to_table})
            )
            RETURN [node IN nodes(path) | node.name] AS path,
                   length(path) AS hops
        """, {
            "from_table": from_table,
            "to_table"  : to_table
        })

        return result

    # ─────────────────────────────────────────
    #  QUERY 5 — Find most connected tables (hubs)
    # ─────────────────────────────────────────
    def get_most_connected_tables(self) -> list:
        """
        Finds tables with most relationships.
        These are your "hub" tables — central to the data model.

        Usually: users, orders, products are most connected.
        """
        result = self.run("""
            MATCH (t:Table)-[r:RELATED_TO]-()
            RETURN t.name         AS table,
                   COUNT(r)       AS connections,
                   t.row_count    AS rows,
                   t.quality_score AS quality
            ORDER BY connections DESC
            LIMIT 10
        """)

        return result

    # ─────────────────────────────────────────
    #  QUERY 6 — Get all foreign key columns
    # ─────────────────────────────────────────
    def get_all_foreign_keys(self) -> list:
        """
        Lists every foreign key relationship in the database.
        Shows column-level lineage.
        """
        result = self.run("""
            MATCH (src:Column)-[r:REFERENCES]->(tgt:Column)
            RETURN src.table_name  AS from_table,
                   src.name        AS from_column,
                   tgt.table_name  AS to_table,
                   tgt.name        AS to_column
            ORDER BY from_table, from_column
        """)

        return result

    # ─────────────────────────────────────────
    #  QUERY 7 — Search tables by keyword
    # ─────────────────────────────────────────
    def search_tables(self, keyword: str) -> list:
        """
        Searches tables by name, description or tags.

        Example: search_tables("payment")
        → payments, orders (has payment_status column)
        """
        keyword_lower = keyword.lower()
        result = self.run("""
            MATCH (t:Table)
            WHERE toLower(t.name)        CONTAINS $keyword
               OR toLower(t.description) CONTAINS $keyword
               OR toLower(t.tags)        CONTAINS $keyword
            RETURN t.name          AS table,
                   t.description   AS description,
                   t.quality_score AS quality,
                   t.row_count     AS rows
            ORDER BY t.quality_score DESC
        """, {"keyword": keyword_lower})

        return result

    def close(self):
        self.driver.close()


# ─────────────────────────────────────────────
#  DEMO — Run all queries and show results
# ─────────────────────────────────────────────
def run_demo():
    print("\n" + "="*60)
    print("  🔍 Lineage Queries Demo")
    print("="*60)

    lq = LineageQueries()

    # ── Query 1: Related tables for orders ──
    print("\n  📋 Query 1: Tables related to 'orders'")
    print("  " + "─"*45)
    results = lq.get_related_tables("orders")
    if results:
        for r in results:
            print(f"  → {r['table']:<20} rows: {r['rows']:<8} via: {r['via']}")
    else:
        print("  No results found")

    # ── Query 2: Upstream of orders ──
    print("\n  ⬆️  Query 2: Upstream tables (feeds INTO orders)")
    print("  " + "─"*45)
    results = lq.get_upstream("orders")
    if results:
        for r in results:
            print(f"  → {r['upstream_table']:<20} {r['from_column']} → {r['to_column']}")
    else:
        print("  No upstream tables found")

    # ── Query 3: Downstream of users ──
    print("\n  ⬇️  Query 3: Downstream tables (uses data FROM users)")
    print("  " + "─"*45)
    results = lq.get_downstream("users")
    if results:
        for r in results:
            print(f"  → {r['downstream_table']:<20} via {r['from_column']}")
    else:
        print("  No downstream tables found")

    # ── Query 4: Path between tables ──
    print("\n  🛤️  Query 4: Lineage path from user_events to payments")
    print("  " + "─"*45)
    results = lq.get_lineage_path("user_events", "payments")
    if results:
        for r in results:
            path = " → ".join(r["path"])
            print(f"  {path}")
            print(f"  Hops: {r['hops']}")
    else:
        print("  No path found")

    # ── Query 5: Most connected tables ──
    print("\n  🏆 Query 5: Most connected tables (hub tables)")
    print("  " + "─"*45)
    results = lq.get_most_connected_tables()
    if results:
        for r in results:
            print(f"  {r['table']:<20} connections: {r['connections']}")
    else:
        print("  No results found")

    # ── Query 6: All foreign keys ──
    print("\n  🔑 Query 6: All foreign key relationships")
    print("  " + "─"*45)
    results = lq.get_all_foreign_keys()
    if results:
        for r in results:
            print(f"  {r['from_table']}.{r['from_column']:<30} → {r['to_table']}.{r['to_column']}")
    else:
        print("  No foreign keys found")

    # ── Query 7: Search ──
    print("\n  🔍 Query 7: Search tables containing 'payment'")
    print("  " + "─"*45)
    results = lq.search_tables("payment")
    if results:
        for r in results:
            print(f"  {r['table']:<20} quality: {r['quality']} | {r['description'][:50]}...")
    else:
        print("  No results found")

    print("\n" + "="*60)
    print("  ✅ All lineage queries executed successfully!")
    print("="*60)
    print("\n  🌐 Open Neo4j browser to see visual graph:")
    print("     http://localhost:7474")
    print("     Run: MATCH (n) RETURN n LIMIT 100\n")

    lq.close()


if __name__ == "__main__":
    run_demo()