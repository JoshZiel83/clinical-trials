"""Row counts for every table/view in the live DuckDB.

Repeatable source of truth for reconciling `data/DATABASE_SCHEMA.md` (A4 / #6).
Run after a pipeline refresh:

    python -m scripts.schema_counts
"""

from config.settings import get_duckdb_connection

SCHEMAS = ("raw", "enriched", "class", "norm", "entities", "ref", "views", "meta")


def collect_counts(conn):
    """Return ``[(schema, name, table_type, row_count), ...]`` ordered."""
    objects = conn.execute(
        """
        SELECT table_schema, table_name, table_type
        FROM information_schema.tables
        WHERE table_schema IN (SELECT unnest(?))
        ORDER BY table_schema, table_name
        """,
        [list(SCHEMAS)],
    ).fetchall()
    out = []
    for schema, name, ttype in objects:
        try:
            n = conn.execute(f'SELECT count(*) FROM "{schema}"."{name}"').fetchone()[0]
        except Exception as exc:  # pragma: no cover - surfaced in output
            n = f"ERROR: {exc}"
        out.append((schema, name, ttype, n))
    return out


def main():
    conn = get_duckdb_connection(read_only=True)
    try:
        rows = collect_counts(conn)
    finally:
        conn.close()

    current_schema = None
    for schema, name, ttype, n in rows:
        if schema != current_schema:
            print(f"\n[{schema}]")
            current_schema = schema
        tag = " (view)" if ttype == "VIEW" else ""
        count = f"{n:,}" if isinstance(n, int) else n
        print(f"  {name}{tag}: {count}")


if __name__ == "__main__":
    main()
