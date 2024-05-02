from dotenv import load_dotenv
load_dotenv()
import os
import sys
sys.path.append('/home/r42/caseWill')
from datetime import datetime

from utils.connections import duckdb_postgres_query, postgres_query


PATH_SILVER = os.getenv('MINIO_silver')
FILE_NAME = datetime.now().strftime('%Y-%m-%d')


query = f"""
    CREATE TABLE IF NOT EXISTS silver.pix_inconsistencies (
        transaction_id          VARCHAR     NULL,
        transaction_date        DATE        NULL,
        transaction_month       INTEGER     NULL,
        customer_id             INTEGER     NULL,
        cd_seqlan               INTEGER     NULL,
        transaction_type        VARCHAR     NULL,
        transaction_value       DOUBLE      NULL,
        customer_name           VARCHAR     NULL,
        customer_uf             VARCHAR     NULL,
        customer_street         VARCHAR     NULL,
        year                    INTEGER     NOT NULL,
        month                   INTEGER     NOT NULL,
        day                     INTEGER     NOT NULL
    );

    CREATE OR REPLACE TEMP VIEW temp_view_pix_inconsistencies AS (
        SELECT
            ca.id_transaction as transaction_id,
            ca.dt_transaction as transaction_date,
            ca.dt_month as transaction_month,
            ca.surrogate_key as customer_id,
            ca.cd_seqlan as cd_seqlan,
            ca.ds_transaction_type as transaction_type,
            ca.vl_transaction as transaction_value,
            cu.full_name as customer_name,
            cu.uf as customer_uf,
            cu.street_name as customer_street,
            EXTRACT(YEAR FROM ca.dt_transaction) AS year,
            EXTRACT(MONTH FROM ca.dt_transaction) AS month,
            EXTRACT(DAY FROM ca.dt_transaction) AS day
        FROM
            bronze.core_account ca
        LEFT JOIN
            bronze.core_pix cp
        ON
            ca.id_transaction = cp.id_transaction
        JOIN
            bronze.customer cu
        ON
            ca.surrogate_key = cu.surrogate_key
        WHERE
            cp.id_transaction IS NULL
    );
    
    INSERT INTO silver.pix_inconsistencies (
        transaction_id,
        transaction_date,
        transaction_month,
        customer_id,
        cd_seqlan,
        transaction_type,
        transaction_value,
        customer_name,
        customer_uf,
        customer_street,
        year,
        month,
        day
    )
    SELECT * FROM temp_view_pix_inconsistencies;
    
    COPY (SELECT * FROM silver.pix_inconsistencies)
    TO '{PATH_SILVER}/pix_inconsistencies/{FILE_NAME}.parquet' (
        FORMAT PARQUET
    );
"""
comments = """
    COMMENT ON TABLE silver.pix_inconsistencies IS 'Tabela para armazenar transações PIX que apresentam inconsistências. Cada registro representa uma transação PIX que não foi capturada corretamente no sistema de registro Core PIX. Esta tabela ajuda a equipe técnica a identificar e corrigir essas inconsistências caso a caso.';
    COMMENT ON COLUMN silver.pix_inconsistencies.transaction_id IS 'Identificador único da transação';
    COMMENT ON COLUMN silver.pix_inconsistencies.transaction_date IS 'Data da transação';
    COMMENT ON COLUMN silver.pix_inconsistencies.transaction_month IS 'Mês numérico da transação';
    COMMENT ON COLUMN silver.pix_inconsistencies.customer_id IS 'Identificador único do cliente';
    COMMENT ON COLUMN silver.pix_inconsistencies.cd_seqlan IS 'Código sequencial da lançamento';
    COMMENT ON COLUMN silver.pix_inconsistencies.transaction_type IS 'Tipo da transação';
    COMMENT ON COLUMN silver.pix_inconsistencies.transaction_value IS 'Valor da transação';
    COMMENT ON COLUMN silver.pix_inconsistencies.customer_name IS 'Nome do cliente';
    COMMENT ON COLUMN silver.pix_inconsistencies.customer_uf IS 'Unidade Federativa do cliente';
    COMMENT ON COLUMN silver.pix_inconsistencies.customer_street IS 'Rua do cliente';
    COMMENT ON COLUMN silver.pix_inconsistencies.year IS 'Ano da transação';
    COMMENT ON COLUMN silver.pix_inconsistencies.month IS 'Mês da transação';
    COMMENT ON COLUMN silver.pix_inconsistencies.day IS 'Dia da transação';
"""

duckdb_postgres_query(query)
postgres_query(comments)