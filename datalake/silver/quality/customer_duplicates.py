from dotenv import load_dotenv
load_dotenv()
import os
import sys
sys.path.append('/home/r42/caseWill')
from datetime import datetime

from utils.connections import duckdb_postgres_query, postgres_query


PATH_SILVER = os.getenv('MINIO_SILVER')
FILE_NAME = datetime.now().strftime('%Y-%m-%d')


query = f"""
    CREATE TABLE IF NOT EXISTS silver.customer_id_duplicates (
        customer_id          INTEGER     NULL,
        entry_date           DATE        NULL,
        birth_date           DATE        NULL,
        uf_name              VARCHAR     NULL,
        uf                   VARCHAR     NULL
    );
    
    CREATE OR REPLACE TEMP VIEW temp_view_silver_customer_id_duplicates AS (
        SELECT
            surrogate_key,
            entry_date,
            birth_date,
            uf_name,
            uf
        FROM (
            SELECT
                *,   
                COUNT(1) OVER (PARTITION BY surrogate_key) AS rn
            FROM
                bronze.customer
        )
        WHERE
            rn <> 1
    );
    
    INSERT INTO silver.customer_id_duplicates (
        customer_id,
        entry_date,
        birth_date,
        uf_name,
        uf
    )
    SELECT * FROM temp_view_silver_customer_id_duplicates;
    
    COPY (SELECT * FROM silver.customer_id_duplicates)
    TO '{PATH_SILVER}/customer_id_duplicates/{FILE_NAME}.parquet' (
        FORMAT PARQUET
    );
"""
comments = """
    COMMENT ON TABLE silver.customer_id_duplicates IS 'Tabela para armazenar informações da tabela de origem customer com id duplicados. Esta tabela ajuda a equipe técnica a identificar e corrigir essas inconsistências caso a caso.';
    COMMENT ON COLUMN silver.customer_id_duplicates.customer_id IS 'Identificação única do cliente';
    COMMENT ON COLUMN silver.customer_id_duplicates.entry_date IS 'Data de abertura da conta';
    COMMENT ON COLUMN silver.customer_id_duplicates.birth_date IS 'Data de nascimento';
    COMMENT ON COLUMN silver.customer_id_duplicates.uf_name IS 'Estado de residência';
    COMMENT ON COLUMN silver.customer_id_duplicates.uf IS 'Sigla do estado de residência';
"""

duckdb_postgres_query(query)
postgres_query(comments)