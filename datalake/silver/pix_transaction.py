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
    CREATE TABLE IF NOT EXISTS silver.pix_transaction (
        transaction_id          VARCHAR     PRIMARY KEY,
        transaction_date        DATE        NULL,
        transaction_month       INTEGER     NULL,
        customer_id             INTEGER     NULL,
        cd_seqlan               INTEGER     NULL,
        transaction_type        VARCHAR     NULL,
        transaction_value       DOUBLE      NULL,
        year                    INTEGER     NOT NULL,
        month                   INTEGER     NOT NULL,
        day                     INTEGER     NOT NULL,
    );

    CREATE OR REPLACE TEMP VIEW temp_view_silver_pix_transaction AS (
        SELECT
            a.transaction_id,
            a.transaction_date,
            a.transaction_month,
            a.customer_id,
            a.cd_seqlan,
            a.transaction_type,
            a.transaction_value,
            a.year,
            a.month,
            a.day
        FROM
            silver.core_account a
        JOIN
            silver.core_pix p
        ON
            a.transaction_id = p.transaction_id
    );

    INSERT INTO silver.pix_transaction
    SELECT *
    FROM temp_view_silver_pix_transaction;

    COPY (SELECT * FROM silver.pix_transaction)
    TO '{PATH_SILVER}/pix_transaction/{FILE_NAME}.parquet' (
        FORMAT PARQUET,
        OVERWRITE_OR_IGNORE true
    );
"""

comments = """
    COMMENT ON TABLE silver.pix_transaction IS 'Tabela que agrega informações das transações PIX com detalhes complementares da conta core, fornecendo uma visão completa de cada transação PIX FINALIZADA e registrada nos sistemas Silver.';
    COMMENT ON COLUMN silver.pix_transaction.transaction_id IS 'Identificador único da transação PIX, compartilhado entre as tabelas core_pix e core_account.';
    COMMENT ON COLUMN silver.pix_transaction.transaction_date IS 'Data em que a transação foi registrada na tabela core_account.';
    COMMENT ON COLUMN silver.pix_transaction.transaction_month IS 'Mês numérico em que a transação foi registrada, derivado da data da transação na tabela core_account.';
    COMMENT ON COLUMN silver.pix_transaction.customer_id IS 'Identificador único do cliente associado à transação.';
    COMMENT ON COLUMN silver.pix_transaction.cd_seqlan IS 'Código sequencial do lançamento da transação.';
    COMMENT ON COLUMN silver.pix_transaction.transaction_type IS 'Tipo da transação registrada, especificando se é PIX ou outro tipo de transação.';
    COMMENT ON COLUMN silver.pix_transaction.transaction_value IS 'Valor monetário da transação conforme registrado na tabela core_account.';
    COMMENT ON COLUMN silver.pix_transaction.year IS 'Partição de Ano extraído da data da transação na tabela core_account.';
    COMMENT ON COLUMN silver.pix_transaction.month IS 'Partição de Mês extraído da data da transação na tabela core_account.';
    COMMENT ON COLUMN silver.pix_transaction.day IS 'Partição de Dia extraído da data da transação na tabela core_account.';
"""

duckdb_postgres_query(query)
postgres_query(comments)