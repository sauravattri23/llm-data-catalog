"""
=============================================================
  schema_parser.py
  Converts raw PostgreSQL data types into clean,
  structured Python dictionaries easy to work with
=============================================================
"""


# ─────────────────────────────────────────────
#  Maps raw PostgreSQL types → clean type names
# ─────────────────────────────────────────────
TYPE_MAP = {
    "integer"                    : "integer",
    "bigint"                     : "integer",
    "smallint"                   : "integer",
    "serial"                     : "integer",
    "bigserial"                  : "integer",
    "numeric"                    : "decimal",
    "decimal"                    : "decimal",
    "real"                       : "float",
    "double precision"           : "float",
    "float"                      : "float",
    "character varying"          : "string",
    "varchar"                    : "string",
    "character"                  : "string",
    "char"                       : "string",
    "text"                       : "text",
    "boolean"                    : "boolean",
    "timestamp without time zone": "timestamp",
    "timestamp with time zone"   : "timestamp",
    "timestamp"                  : "timestamp",
    "date"                       : "date",
    "time without time zone"     : "time",
    "time with time zone"        : "time",
    "json"                       : "json",
    "jsonb"                      : "json",
    "uuid"                       : "uuid",
    "bytea"                      : "binary",
    "array"                      : "array",
}


def parse_data_type(raw_type: str) -> dict:
    """
    Converts raw PostgreSQL type string into clean dict.

    Example:
        Input:  "character varying(150)"
        Output: { "clean_type": "string", "max_length": 150, "raw_type": "character varying(150)" }
    """
    raw_type = raw_type.lower().strip()
    result = {
        "raw_type"   : raw_type,
        "clean_type" : "unknown",
        "max_length" : None,
        "precision"  : None,
        "scale"      : None,
    }

    # Extract length from varchar(150) or numeric(10,2)
    if "(" in raw_type:
        base_type = raw_type[:raw_type.index("(")].strip()
        params    = raw_type[raw_type.index("(")+1 : raw_type.index(")")].strip()

        if "," in params:
            parts             = params.split(",")
            result["precision"] = int(parts[0].strip())
            result["scale"]     = int(parts[1].strip())
        else:
            result["max_length"] = int(params) if params.isdigit() else None
    else:
        base_type = raw_type

    result["clean_type"] = TYPE_MAP.get(base_type, "unknown")
    return result


def parse_column(raw_column: dict) -> dict:
    """
    Takes a raw column dict from SQLAlchemy inspection
    and returns a clean, structured column dict.

    Input (raw):
        {
            "name": "email",
            "type": "VARCHAR(150)",
            "nullable": False,
            "default": None,
            "primary_key": False
        }

    Output (clean):
        {
            "column_name": "email",
            "clean_type": "string",
            "max_length": 150,
            "is_nullable": False,
            "has_default": False,
            "is_primary_key": False,
            "raw_type": "varchar(150)"
        }
    """
    type_info = parse_data_type(str(raw_column.get("type", "")))

    return {
        "column_name"   : raw_column.get("name"),
        "clean_type"    : type_info["clean_type"],
        "max_length"    : type_info["max_length"],
        "precision"     : type_info["precision"],
        "scale"         : type_info["scale"],
        "raw_type"      : type_info["raw_type"],
        "is_nullable"   : raw_column.get("nullable", True),
        "has_default"   : raw_column.get("default") is not None,
        "default_value" : str(raw_column.get("default")) if raw_column.get("default") else None,
        "is_primary_key": raw_column.get("primary_key", False),
        "is_foreign_key": False,   # updated later by metadata_extractor
        "references"    : None,    # updated later by metadata_extractor
    }


def parse_foreign_key(fk_raw: dict) -> dict:
    """
    Parses a raw foreign key constraint into clean format.

    Output:
        {
            "column": "user_id",
            "references_table": "users",
            "references_column": "user_id"
        }
    """
    referred_table  = fk_raw.get("referred_table", "")
    constrained_cols= fk_raw.get("constrained_columns", [])
    referred_cols   = fk_raw.get("referred_columns", [])

    return {
        "column"            : constrained_cols[0] if constrained_cols else None,
        "references_table"  : referred_table,
        "references_column" : referred_cols[0] if referred_cols else None,
    }


def parse_table_summary(table_name: str, columns: list, foreign_keys: list) -> dict:
    """
    Creates a complete table summary dict combining
    all parsed columns and foreign key relationships.
    """
    # Mark foreign key columns
    fk_column_map = {fk["column"]: fk for fk in foreign_keys}
    for col in columns:
        if col["column_name"] in fk_column_map:
            col["is_foreign_key"] = True
            col["references"]     = fk_column_map[col["column_name"]]

    return {
        "table_name"      : table_name,
        "column_count"    : len(columns),
        "columns"         : columns,
        "foreign_keys"    : foreign_keys,
        "primary_key_cols": [c["column_name"] for c in columns if c["is_primary_key"]],
        "nullable_cols"   : [c["column_name"] for c in columns if c["is_nullable"]],
        "required_cols"   : [c["column_name"] for c in columns if not c["is_nullable"]],
    }


if __name__ == "__main__":
    # Quick test
    print("Testing schema_parser...\n")

    test_types = [
        "character varying(150)",
        "integer",
        "numeric(10,2)",
        "timestamp without time zone",
        "boolean",
        "text",
        "bigint",
    ]

    for t in test_types:
        result = parse_data_type(t)
        print(f"  {t:40} → {result['clean_type']}")

    print("\n✅ schema_parser working correctly!")