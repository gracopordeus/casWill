import sys
sys.path.append('/home/r42/caseWill')

from utils.connections import duckdb_postgres_query

create_schema_bronze = """
    CREATE SCHEMA IF NOT EXISTS postgres.bronze;
"""

create_schema_silver = """
    CREATE SCHEMA IF NOT EXISTS postgres.silver;
"""

create_schema_golden = """
    CREATE SCHEMA IF NOT EXISTS postgres.golden;
"""

create_schemas = [
    create_schema_bronze,
    create_schema_silver,
    create_schema_golden
]

for schema in create_schemas:
    duckdb_postgres_query(schema)
    
print("Done!")