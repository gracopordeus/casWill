import sys
sys.path.append('/home/r42/caseWill')

from utils.connections import duckdb_load_postgres
db = duckdb_load_postgres()

create_schema_bronze = """
    CREATE SCHEMA IF NOT EXISTS bronze;
"""

create_schema_silver = """
    CREATE SCHEMA IF NOT EXISTS silver;
"""

create_schema_golden = """
    CREATE SCHEMA IF NOT EXISTS golden;
"""

create_schemas = [
    create_schema_bronze,
    create_schema_silver,
    create_schema_golden
]

for schema in create_schemas:
    db.sql("USE postgres; "+schema)
    
print("Done!")