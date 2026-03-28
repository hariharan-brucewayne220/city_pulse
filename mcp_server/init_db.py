"""
Initialize DuckDB with pre-cached civic datasets.
Call init_database() to get a ready connection.
"""
import os
import duckdb

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

TABLES = {
    "algorithmic_tools": "algorithmic_tools.csv",
    "hmda_nyc": "hmda_nyc.csv",
    "delivery_workers": "delivery_workers.csv",
    "ppp_nyc": "ppp_nyc.csv",
}


def init_database(db_path: str = ":memory:") -> duckdb.DuckDBPyConnection:
    """Load all CSV datasets into DuckDB. Returns a read-only-ish connection."""
    conn = duckdb.connect(db_path)
    for table_name, filename in TABLES.items():
        csv_path = os.path.join(DATA_DIR, filename)
        if not os.path.exists(csv_path):
            raise FileNotFoundError(
                f"Dataset missing: {csv_path}\n"
                f"Run: python scripts/generate_data.py"
            )
        conn.execute(
            f"CREATE OR REPLACE TABLE {table_name} AS "
            f"SELECT * FROM read_csv_auto('{csv_path}')"
        )
    return conn


if __name__ == "__main__":
    conn = init_database()
    print("=== DuckDB tables loaded ===")
    for table in TABLES:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        cols = [c[0] for c in conn.execute(f"DESCRIBE {table}").fetchall()]
        print(f"\n{table}: {count} rows")
        print(f"  columns: {', '.join(cols)}")
    conn.close()
