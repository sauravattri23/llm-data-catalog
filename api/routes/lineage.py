"""
=============================================================
  routes/lineage.py
  Data lineage endpoints using Neo4j graph database.
=============================================================
"""

from fastapi import APIRouter, HTTPException
from neo4j import GraphDatabase
from typing import Optional
import os
from dotenv import load_dotenv

load_dotenv()

router = APIRouter()

NEO4J_URI  = os.getenv("NEO4J_URI",  "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASS = os.getenv("NEO4J_PASS", "neo4j_pass")


def get_neo4j():
    """Returns Neo4j driver connection."""
    return GraphDatabase.driver(
        NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS)
    )


def run_query(query: str, params: dict = {}) -> list:
    """Runs a Cypher query and returns results."""
    driver = get_neo4j()
    with driver.session() as session:
        result = session.run(query, params).data()
    driver.close()
    return result


@router.get("/relationships")
def get_all_relationships():
    """
    Get all foreign key relationships in the database.
    Shows complete lineage map.
    """
    try:
        result = run_query("""
            MATCH (src:Column)-[r:REFERENCES]->(tgt:Column)
            RETURN
                src.table_name  AS from_table,
                src.name        AS from_column,
                tgt.table_name  AS to_table,
                tgt.name        AS to_column
            ORDER BY from_table, from_column
        """)
        return {
            "total"        : len(result),
            "relationships": result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tables")
def get_all_table_relationships():
    """
    Get all table-level relationships.
    Shows which tables are connected to which.
    """
    try:
        result = run_query("""
            MATCH (src:Table)-[r:RELATED_TO]->(tgt:Table)
            RETURN
                src.name AS source_table,
                tgt.name AS target_table,
                r.via    AS via,
                src.row_count AS source_rows,
                tgt.row_count AS target_rows
            ORDER BY source_table
        """)
        return {
            "total"        : len(result),
            "relationships": result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/hubs")
def get_hub_tables(limit: Optional[int] = 10):
    """
    Get most connected tables (hub tables).
    These are central tables in your data model.
    """
    try:
        result = run_query("""
            MATCH (t:Table)-[r:RELATED_TO]-()
            RETURN
                t.name          AS table_name,
                COUNT(r)        AS connections,
                t.row_count     AS rows,
                t.quality_score AS quality_score,
                t.description   AS description
            ORDER BY connections DESC
            LIMIT $limit
        """, {"limit": limit})
        return {
            "hub_tables": result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{table_name}/upstream")
def get_upstream(table_name: str):
    """
    Get upstream tables — tables that feed data INTO this table.
    Shows where this table's data comes from.
    """
    try:
        result = run_query("""
            MATCH (src:Table {name: $table_name})-[:HAS_COLUMN]->
                  (col:Column)-[:REFERENCES]->(ref:Column)<-
                  [:HAS_COLUMN]-(tgt:Table)
            RETURN DISTINCT
                tgt.name        AS upstream_table,
                col.name        AS from_column,
                ref.name        AS to_column,
                tgt.row_count   AS rows,
                tgt.description AS description
            ORDER BY upstream_table
        """, {"table_name": table_name})

        if not result and table_name:
            # Verify table exists
            check = run_query("""
                MATCH (t:Table {name: $name}) RETURN t.name AS name
            """, {"name": table_name})
            if not check:
                raise HTTPException(
                    status_code = 404,
                    detail      = f"Table '{table_name}' not found in lineage graph"
                )

        return {
            "table"         : table_name,
            "upstream_count": len(result),
            "upstream"      : result
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{table_name}/downstream")
def get_downstream(table_name: str):
    """
    Get downstream tables — tables that USE data FROM this table.
    Shows where this table's data goes.
    """
    try:
        result = run_query("""
            MATCH (src:Table {name: $table_name})-[:HAS_COLUMN]->
                  (col:Column)<-[:REFERENCES]-(ref:Column)<-
                  [:HAS_COLUMN]-(tgt:Table)
            RETURN DISTINCT
                tgt.name        AS downstream_table,
                ref.name        AS from_column,
                col.name        AS to_column,
                tgt.row_count   AS rows,
                tgt.description AS description
            ORDER BY downstream_table
        """, {"table_name": table_name})

        return {
            "table"           : table_name,
            "downstream_count": len(result),
            "downstream"      : result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{table_name}/path")
def get_lineage_path(
    table_name: str,
    to: str
):
    """
    Get shortest lineage path between two tables.
    Shows how two tables are connected.

    Example: /lineage/user_events/path?to=payments
    """
    try:
        result = run_query("""
            MATCH path = shortestPath(
                (start:Table {name: $from_table})-[*]-(end:Table {name: $to_table})
            )
            RETURN
                [node IN nodes(path) | node.name] AS path,
                length(path) AS hops
        """, {
            "from_table": table_name,
            "to_table"  : to
        })

        if not result:
            return {
                "from"   : table_name,
                "to"     : to,
                "path"   : [],
                "hops"   : None,
                "message": f"No path found between {table_name} and {to}"
            }

        return {
            "from" : table_name,
            "to"   : to,
            "path" : result[0]["path"],
            "hops" : result[0]["hops"]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{table_name}/graph")
def get_table_graph(table_name: str):
    """
    Get complete graph data for a table.
    Returns all connected nodes and relationships.
    Useful for frontend visualization.
    """
    try:
        # Get table info
        table_info = run_query("""
            MATCH (t:Table {name: $name})
            RETURN t.name AS name, t.row_count AS rows,
                   t.quality_score AS quality, t.description AS description
        """, {"name": table_name})

        if not table_info:
            raise HTTPException(
                status_code = 404,
                detail      = f"Table '{table_name}' not found"
            )

        # Get connected tables
        connected = run_query("""
            MATCH (t:Table {name: $name})-[r:RELATED_TO]-(other:Table)
            RETURN other.name AS table, type(r) AS relationship,
                   other.row_count AS rows, other.quality_score AS quality
        """, {"name": table_name})

        # Get columns
        columns = run_query("""
            MATCH (t:Table {name: $name})-[:HAS_COLUMN]->(c:Column)
            RETURN c.name AS column, c.data_type AS type,
                   c.is_primary_key AS is_pk, c.is_foreign_key AS is_fk
        """, {"name": table_name})

        return {
            "table"    : table_info[0] if table_info else {},
            "connected": connected,
            "columns"  : columns
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))