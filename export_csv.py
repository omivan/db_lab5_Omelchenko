import psycopg2
import pandas as pd


db_params = {
    "host": "localhost",
    "database": "test_db_lab",
    "user": "omivan",
    "password": "0104",
    "port": "5432",
}

def export_table_to_csv(conn, table_name):
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql_query(query, conn)
    df.to_csv(f"csv_saved_files/{table_name}.csv", index=False)

try:
    conn = psycopg2.connect(**db_params)

    # Export each table to CSV
    table_names = ["game", "company", "genre", "publish", "develop", "game_genre"]
    for table_name in table_names:
        export_table_to_csv(conn, table_name)

    print("CSV files exported successfully.")

except Exception as e:
    print(f"Error: {e}")

finally:
    # Close the database connection
    if conn:
        conn.close()