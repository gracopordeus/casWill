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
    CREATE TABLE IF NOT EXISTS silver.customer (
        customer_id          INTEGER     PRIMARY KEY,
        entry_date           DATE        NULL,
        birth_date           DATE        NULL,
        uf_name              VARCHAR     NULL,
        uf                   VARCHAR     NULL
    );
    
    CREATE OR REPLACE TEMP VIEW temp_view_silver_customer AS (
        SELECT
            surrogate_key,
            entry_date,
            birth_date,
            uf_name,
            uf,
        FROM (
            SELECT
                *,   
                COUNT(1) OVER (PARTITION BY surrogate_key) AS rn
            FROM
                bronze.customer
        ) subquery
        WHERE
            rn = 1
    );
    
    INSERT INTO silver.customer (
        customer_id,
        entry_date,
        birth_date,
        uf_name,
        uf
    )
    SELECT * FROM temp_view_silver_customer;
    
    COPY (SELECT * FROM silver.customer)
    TO '{PATH_SILVER}/customer/{FILE_NAME}.parquet' (
        FORMAT PARQUET,
        OVERWRITE_OR_IGNORE true
    );
"""
comments = """
    COMMENT ON TABLE silver.customer IS 'Esta tabela contém informações detalhadas sobre os clientes, incluindo dados pessoais e de contato. É essencial para a gestão de relacionamentos com clientes e suporta operações de marketing, vendas e atendimento ao cliente.';
    COMMENT ON COLUMN silver.customer.customer_id IS 'Identificação única do cliente';
    COMMENT ON COLUMN silver.customer.entry_date IS 'Data de abertura da conta';
    COMMENT ON COLUMN silver.customer.birth_date IS 'Data de nascimento';
    COMMENT ON COLUMN silver.customer.uf_name IS 'Estado de residência';
    COMMENT ON COLUMN silver.customer.uf IS 'Sigla do estado de residência';
"""

duckdb_postgres_query(query)
postgres_query(comments)