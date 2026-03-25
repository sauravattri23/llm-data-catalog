"""
=============================================================
  catalog_generator.py
  Main LLM engine — reads metadata from catalog database,
  generates AI descriptions using HuggingFace flan-t5-large,
  saves descriptions back to catalog database.
=============================================================
"""

import os
import time
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from transformers import T5ForConditionalGeneration, T5Tokenizer
from dotenv import load_dotenv

from prompt_templates import (
    get_table_description_prompt,
    get_column_description_prompt,
    get_table_tags_prompt,
    get_relationship_description_prompt
)
from response_cache import get_cached_response, save_to_cache, get_cache_stats

load_dotenv()

# ─────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://catalog_user:catalog_pass@localhost:5432/ecommerce_db"
)

# HuggingFace model — free, runs locally on your laptop
# flan-t5-large gives better quality than base
MODEL_NAME = "google/flan-t5-large"


# ─────────────────────────────────────────────
#  LLM MODEL CLASS
# ─────────────────────────────────────────────
class LocalLLM:
    """
    Wrapper around HuggingFace flan-t5-large model.
    Downloads model once, runs locally forever after.
    """

    def __init__(self):
        self.model     = None
        self.tokenizer = None
        self.loaded    = False

    def load(self):
        """
        Downloads and loads the model into memory.
        First run: downloads ~800MB to your laptop.
        After that: loads from local cache instantly.
        """
        if self.loaded:
            return

        print(f"\n  🤖 Loading HuggingFace model: {MODEL_NAME}")
        print(f"  ⏳ First time = downloads ~800MB (only once)")
        print(f"  ⏳ After that = loads from cache in ~10 seconds\n")

        self.tokenizer = T5Tokenizer.from_pretrained(MODEL_NAME)
        self.model     = T5ForConditionalGeneration.from_pretrained(MODEL_NAME)
        self.loaded    = True

        print(f"  ✅ Model loaded successfully!\n")

    def generate(self, prompt: str, max_length: int = 150) -> str:
        """
        Sends a prompt to the model and returns the response.

        Args:
            prompt    : The text prompt to send
            max_length: Max words in response (default 150)

        Returns:
            Generated text string
        """
        if not self.loaded:
            self.load()

        # Check cache first — don't call model if we have this already
        cached = get_cached_response(prompt)
        if cached:
            return cached

        # Tokenize prompt (convert text to numbers model understands)
        inputs = self.tokenizer(
            prompt,
            return_tensors = "pt",   # PyTorch tensors
            max_length     = 512,    # max prompt length
            truncation     = True,   # cut if too long
        )

        # Generate response
        outputs = self.model.generate(
            inputs["input_ids"],
            max_length         = max_length,
            num_beams          = 4,      # beam search for better quality
            early_stopping     = True,   # stop when answer is complete
            no_repeat_ngram_size = 2,    # avoid repeating phrases
        )

        # Decode response (convert numbers back to text)
        response = self.tokenizer.decode(
            outputs[0],
            skip_special_tokens = True
        ).strip()

        # Save to cache for next time
        save_to_cache(prompt, response)

        return response


# ─────────────────────────────────────────────
#  DATABASE HELPER FUNCTIONS
# ─────────────────────────────────────────────

def get_all_catalog_tables(engine) -> list:
    """Reads all tables from catalog_tables."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT table_name, row_count, column_count
            FROM catalog_tables
            ORDER BY row_count DESC
        """))
        return [dict(row._mapping) for row in result]


def get_table_columns(engine, table_name: str) -> list:
    """Gets all columns for a table from catalog_columns."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT column_name, clean_type, null_pct
            FROM catalog_columns
            WHERE table_name = :table_name
            ORDER BY column_name
        """), {"table_name": table_name})
        return [dict(row._mapping) for row in result]


def get_sample_values(engine, table_name: str,
                       column_name: str, clean_type: str) -> list:
    """
    Gets top 3 most common values from a column.
    Only works for string/boolean columns — not for IDs or timestamps.
    """
    # Skip columns where sample values aren't meaningful
    skip_patterns = ["_id", "_at", "_date", "password", "token", "hash"]
    if any(p in column_name.lower() for p in skip_patterns):
        return []

    if clean_type not in ["string", "boolean", "integer"]:
        return []

    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT {column_name}, COUNT(*) as freq
                FROM {table_name}
                WHERE {column_name} IS NOT NULL
                GROUP BY {column_name}
                ORDER BY freq DESC
                LIMIT 3
            """))
            return [str(row[0]) for row in result]
    except Exception:
        return []


def update_table_description(engine, table_name: str,
                               description: str, tags: str):
    """Saves AI description and tags back to catalog_tables."""
    with engine.connect() as conn:
        conn.execute(text("""
            UPDATE catalog_tables
            SET ai_description = :description,
                ai_tags        = :tags
            WHERE table_name   = :table_name
        """), {
            "description": description,
            "tags"       : tags,
            "table_name" : table_name
        })
        conn.commit()


def update_column_description(engine, table_name: str,
                                column_name: str, description: str):
    """Saves AI description back to catalog_columns."""
    with engine.connect() as conn:
        conn.execute(text("""
            UPDATE catalog_columns
            SET ai_description = :description
            WHERE table_name   = :table_name
            AND   column_name  = :column_name
        """), {
            "description": description,
            "table_name" : table_name,
            "column_name": column_name
        })
        conn.commit()


# ─────────────────────────────────────────────
#  MAIN GENERATOR CLASS
# ─────────────────────────────────────────────

class CatalogGenerator:
    """
    Main class — reads metadata from catalog database,
    generates AI descriptions using flan-t5-large,
    saves everything back to catalog.
    """

    def __init__(self):
        self.engine = create_engine(DATABASE_URL, echo=False)
        self.llm    = LocalLLM()

    def generate_table_description(self, table: dict) -> tuple:
        """
        Generates AI description + tags for one table.

        Returns:
            (description, tags) tuple
        """
        table_name = table["table_name"]

        # Get columns for this table
        columns     = get_table_columns(self.engine, table_name)
        column_names= [c["column_name"] for c in columns]

        # Get sample values for string columns
        sample_values = {}
        for col in columns[:5]:  # only first 5 columns
            samples = get_sample_values(
                self.engine, table_name,
                col["column_name"], col["clean_type"]
            )
            if samples:
                sample_values[col["column_name"]] = samples

        # Generate table description
        desc_prompt = get_table_description_prompt(
            table_name   = table_name,
            columns      = column_names,
            row_count    = table["row_count"],
            sample_values= sample_values
        )
        description = self.llm.generate(desc_prompt, max_length=100)

        # Generate tags
        tags_prompt = get_table_tags_prompt(
            table_name     = table_name,
            columns        = column_names,
            ai_description = description
        )
        tags = self.llm.generate(tags_prompt, max_length=50)

        return description, tags

    def generate_column_descriptions(self, table_name: str) -> int:
        """
        Generates AI descriptions for all columns in a table.

        Returns:
            Number of columns processed
        """
        columns = get_table_columns(self.engine, table_name)
        count   = 0

        for col in columns:
            col_name   = col["column_name"]
            clean_type = col["clean_type"]
            null_pct   = col["null_pct"] or 0

            # Get sample values
            samples = get_sample_values(
                self.engine, table_name, col_name, clean_type
            )

            # Generate description
            prompt = get_column_description_prompt(
                table_name    = table_name,
                column_name   = col_name,
                clean_type    = clean_type,
                null_pct      = null_pct,
                sample_values = samples
            )
            description = self.llm.generate(prompt, max_length=80)

            # Save to database
            update_column_description(self.engine, table_name, col_name, description)
            count += 1

        return count

    def run(self):
        """
        Main method — generates descriptions for ALL tables and columns.
        """
        print("\n" + "="*60)
        print("  🤖 LLM Catalog Generator — Phase 3")
        print("="*60)

        # Load the AI model
        self.llm.load()

        # Get all tables from catalog
        tables = get_all_catalog_tables(self.engine)
        print(f"\n  📋 Found {len(tables)} tables to describe\n")

        total_columns = 0
        start_time    = time.time()

        for i, table in enumerate(tables, 1):
            table_name = table["table_name"]
            print(f"  [{i}/{len(tables)}] Processing: {table_name}")

            # Generate table description + tags
            print(f"     📝 Generating table description...")
            description, tags = self.generate_table_description(table)

            # Save table description
            update_table_description(self.engine, table_name, description, tags)
            print(f"     ✅ Table: {description[:60]}...")
            print(f"     🏷️  Tags: {tags}")

            # Generate column descriptions
            print(f"     📝 Generating column descriptions...")
            col_count = self.generate_column_descriptions(table_name)
            total_columns += col_count
            print(f"     ✅ {col_count} columns described\n")

        elapsed = round(time.time() - start_time, 1)

        # Print final summary
        self._print_summary(tables, total_columns, elapsed)

    def _print_summary(self, tables: list, total_columns: int, elapsed: float):
        """Prints beautiful summary of Phase 3 results."""

        # Get cache stats
        cache_stats = get_cache_stats()

        print("="*60)
        print("  ✅ PHASE 3 COMPLETE — AI Descriptions Generated!")
        print("="*60)
        print(f"\n  📊 Summary:")
        print(f"     Tables described:  {len(tables)}")
        print(f"     Columns described: {total_columns}")
        print(f"     Time taken:        {elapsed}s")
        print(f"     Cached responses:  {cache_stats['total_cached']}")

        print(f"\n  📋 Sample descriptions generated:")
        print(f"  {'-'*55}")

        # Show sample results from database
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT table_name, ai_description, ai_tags
                FROM catalog_tables
                WHERE ai_description IS NOT NULL
                LIMIT 5
            """))
            for row in result:
                print(f"\n  Table: {row[0]}")
                print(f"  Desc:  {row[1]}")
                print(f"  Tags:  {row[2]}")

        print(f"\n  🎯 All descriptions saved to:")
        print(f"     → catalog_tables.ai_description")
        print(f"     → catalog_tables.ai_tags")
        print(f"     → catalog_columns.ai_description")
        print(f"\n  🚀 Ready for Phase 4 — Data Lineage with Neo4j!\n")


# ─────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    generator = CatalogGenerator()
    generator.run()