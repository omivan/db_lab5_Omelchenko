import json
import decimal
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import date

class CustomEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        elif isinstance(o, date):
            return o.isoformat()
        return super(CustomEncoder, self).default(o)

db_params = {
    "host": "localhost",
    "database": "test_db_lab",
    "user": "omivan",
    "password": "0104",
    "port": "5432",
}

def export_data_to_json(connection, output_file):
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:

            cursor.execute("SELECT table_name FROM information_schema.tables "
                           "WHERE table_schema = 'public'")
            tables = [table['table_name'] for table in cursor.fetchall()]

            all_data = {}

            for table in tables:

                cursor.execute(f"SELECT * FROM {table}")
                table_data = cursor.fetchall()

                all_data[table] = table_data

            with open(output_file, 'w') as json_file:
                json.dump(all_data, json_file, indent=2, cls=CustomEncoder)

            print(f'Data exported to {output_file} ')

    except psycopg2.Error as e:
        print(f"Error: {e}")

try:
    connection = psycopg2.connect(**db_params)
    export_data_to_json(connection, 'export.json')
except psycopg2.Error as e:
    print(f"PostgreSQL Error: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")
