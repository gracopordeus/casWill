from dotenv import load_dotenv
load_dotenv()
import os
import sys
sys.path.append('/home/r42/caseWill')

from utils.connections import duckdb_postgres_query


PATH_BRONZE = os.getenv('MINIO_bronze')

query = f"""
    CREATE TABLE IF NOT EXISTS bronze.customer (
        entry_date          DATE        NULL,
        surrogate_key       INTEGER     NULL,
        full_name           VARCHAR     NULL,
        birth_date          DATE        NULL,
        uf_name             VARCHAR     NULL,
        uf                  VARCHAR     NULL,
        street_name         VARCHAR     NULL
    );
    
    WITH landing AS (
        SELECT 
            * 
        FROM read_csv_auto('{PATH_BRONZE}/customer.csv')
    )
    INSERT INTO bronze.customer (
        entry_date, 
        surrogate_key, 
        full_name, 
        birth_date, 
        uf_name, 
        uf, 
        street_name
    )
    SELECT * FROM landing
"""

duckdb_postgres_query(query)