"""
=============================================================
  prompt_templates.py
  Contains all prompts sent to the HuggingFace LLM.
  Separate file so prompts are easy to improve later.
=============================================================
"""


def get_table_description_prompt(table_name: str, columns: list,
                                  row_count: int, sample_values: dict) -> str:
    """
    Builds prompt to generate a business description for a table.

    Args:
        table_name   : "orders"
        columns      : ["order_id", "user_id", "order_status"...]
        row_count    : 1000
        sample_values: {"order_status": ["delivered", "shipped", "pending"]}

    Returns:
        A clean prompt string ready to send to the LLM
    """

    # Format columns into readable string
    columns_str = ", ".join(columns[:10])  # max 10 columns to keep prompt short

    # Format sample values
    samples_str = ""
    for col, values in list(sample_values.items())[:3]:  # max 3 columns
        if values:
            samples_str += f"\n- {col}: {', '.join(str(v) for v in values[:3])}"

    prompt = f"""You are a senior data engineer writing documentation for a database catalog.

Write a clear 2-sentence business description for this database table.
Focus on what business data it stores and what it is used for.
Do not mention technical details like data types or row counts.

Table name: {table_name}
Columns: {columns_str}
Row count: {row_count}
Sample values: {samples_str if samples_str else 'Not available'}

Description:"""

    return prompt


def get_column_description_prompt(table_name: str, column_name: str,
                                   clean_type: str, null_pct: float,
                                   sample_values: list) -> str:
    """
    Builds prompt to generate a description for a single column.

    Args:
        table_name   : "orders"
        column_name  : "order_status"
        clean_type   : "string"
        null_pct     : 0.0
        sample_values: ["delivered", "shipped", "pending"]

    Returns:
        A clean prompt string ready to send to the LLM
    """

    # Format sample values
    samples_str = ""
    if sample_values:
        samples_str = ", ".join(str(v) for v in sample_values[:5])

    prompt = f"""You are a senior data engineer writing documentation for a database catalog.

Write a clear 1-sentence description for this database column.
Focus on what business information this column stores.
Do not mention data types or technical implementation details.

Table: {table_name}
Column: {column_name}
Data type: {clean_type}
Nullable: {"Yes" if null_pct > 0 else "No"}
Sample values: {samples_str if samples_str else "Not available"}

Description:"""

    return prompt


def get_table_tags_prompt(table_name: str, columns: list,
                           ai_description: str) -> str:
    """
    Builds prompt to generate searchable tags for a table.

    Returns comma-separated tags like:
    "transactions, payments, e-commerce, orders, fulfillment"
    """

    columns_str = ", ".join(columns[:8])

    prompt = f"""You are a data engineer creating searchable tags for a data catalog.

Generate 5 relevant business tags for this database table.
Return ONLY the tags as comma-separated words. Nothing else.

Table: {table_name}
Columns: {columns_str}
Description: {ai_description}

Tags:"""

    return prompt


def get_relationship_description_prompt(source_table: str,
                                         source_column: str,
                                         target_table: str,
                                         target_column: str) -> str:
    """
    Builds prompt to describe a foreign key relationship
    between two tables in plain English.
    """

    prompt = f"""You are a data engineer explaining database relationships.

Write 1 sentence explaining this database relationship in plain English.
Focus on the business meaning, not the technical implementation.

Relationship:
{source_table}.{source_column} references {target_table}.{target_column}

Explanation:"""

    return prompt