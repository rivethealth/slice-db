MANIFEST_NAME = "manifest.json"

SCHEMA_NAME = "schema.json"

def table_name(table_id: str) -> str:
    return f"{table_id}.tsv"
