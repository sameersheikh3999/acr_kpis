import os
from pathlib import Path

from google.cloud import bigquery
from google.oauth2 import service_account

# BigQuery table: fico_kpis. Override with ACR_DATA_TABLE env var if needed.
ACR_DATA_TABLE = os.getenv("ACR_DATA_TABLE", "tbproddb.fico_kpis").strip()


def get_bigquery_client() -> bigquery.Client:
    """Create an authenticated BigQuery client using the local service account key."""
    key_path = Path(__file__).with_name("keyy.json")
    credentials = service_account.Credentials.from_service_account_file(str(key_path))
    return bigquery.Client(credentials=credentials, project=credentials.project_id)


def show_table_columns():
    """Show the columns and their types for the FICO KPIs table (set ACR_DATA_TABLE env var to override)."""
    if not ACR_DATA_TABLE:
        print("ACR_DATA_TABLE is not set. Default is tbproddb.fico_kpis. To override:")
        print("  set ACR_DATA_TABLE=tbproddb.fico_kpis   (Windows)")
        print("  export ACR_DATA_TABLE=tbproddb.fico_kpis   (Linux/Mac)")
        return
    client = get_bigquery_client()
    table_ref = client.get_table(ACR_DATA_TABLE)
    print(f"Table: {table_ref.full_table_id}")
    print(f"Number of rows: {table_ref.num_rows:,}")
    print(f"\nColumns ({len(table_ref.schema)}):")
    print("-" * 80)
    for field in table_ref.schema:
        mode = f" ({field.mode})" if field.mode != "NULLABLE" else ""
        print(f"{field.name:30} {field.field_type:20}{mode}")


if __name__ == "__main__":
    show_table_columns()