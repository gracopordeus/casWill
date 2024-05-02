from dotenv import load_dotenv
load_dotenv()
import os
import sys
sys.path.append('/home/r42/caseWill')
from datetime import datetime

from utils.connections import duckdb_postgres_query, postgres_query


PATH_BRONZE = os.getenv('MINIO_BRONZE')
FILE_NAME = datetime.now().strftime('%Y-%m-%d')


query = f"""
    CREATE TABLE IF NOT EXISTS bronze.customer (
        cotumer_id           INTEGER     PRIMARY KEY,
        entry_date           DATE        NULL,
        full_name            VARCHAR     NULL,
        birth_date           DATE        NULL,
        uf_name              VARCHAR     NULL,
        uf                   VARCHAR     NULL,
        street_name          VARCHAR     NULL
    );
    
    CREATE OR REPLACE TEMP VIEW temp_view_bronze_customer AS (
        SELECT
            surrogate_key,
            entry_date,
            full_name,
            birth_date,
            uf_name,
            uf,
            street_name
        FROM (
            SELECT
                *,   
                ROW_NUMBER() OVER (PARTITION BY surrogate_key ORDER BY entry_date DESC) AS rn
            FROM
                raw.customer
        ) subquery
        WHERE
            rn = 1
    );
    
    INSERT INTO bronze.customer (
        cotumer_id,
        entry_date,
        full_name,
        birth_date,
        uf_name,
        uf,
        street_name
    )
    SELECT * FROM temp_view_bronze_customer;
    
    COPY (SELECT * FROM bronze.customer)
    TO '{PATH_BRONZE}/customer/{FILE_NAME}.parquet' (
        FORMAT PARQUET
    );
"""
comments = """
    COMMENT ON COLUMN bronze.customer.cotumer_id IS 'Identificação única do cliente';
    COMMENT ON COLUMN bronze.customer.entry_date IS 'Data de abertura da conta';
    COMMENT ON COLUMN bronze.customer.full_name IS 'Nome completo';
    COMMENT ON COLUMN bronze.customer.birth_date IS 'Data de nascimento';
    COMMENT ON COLUMN bronze.customer.uf_name IS 'Estado de residência';
    COMMENT ON COLUMN bronze.customer.uf IS 'Sigla do estado de residência';
    COMMENT ON COLUMN bronze.customer.street_name IS 'Logradouro';
"""

duckdb_postgres_query(query)
postgres_query(comments)