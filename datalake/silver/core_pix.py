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
    CREATE TABLE IF NOT EXISTS silver.core_pix (
        transaction_id          VARCHAR     PRIMARY KEY,
        transaction_date        DATE        NULL,
        transaction_month       INTEGER     NULL,
        cd_seqlan               INTEGER     NULL,
        transaction_type        VARCHAR     NULL,
        transaction_value       DOUBLE      NULL,
        year                    INTEGER     NOT NULL,
        month                   INTEGER     NOT NULL,
        day                     INTEGER     NOT NULL
    );
    
    CREATE OR REPLACE TEMP VIEW temp_view_silver_core_pix AS (
        SELECT
            id_transaction as transaction_id,
            dt_transaction as transaction_date,
            dt_month as transaction_month,
            cd_seqlan as cd_seqlan,
            ds_transaction_type as transaction_type,
            vl_transaction as transaction_value,
            year,
            month,
            day
        FROM (
            SELECT
                *,
                EXTRACT(YEAR FROM dt_transaction) AS year,
                EXTRACT(MONTH FROM dt_transaction) AS month,
                EXTRACT(DAY FROM dt_transaction) AS day,
                COUNT(1) OVER (PARTITION BY id_transaction) AS rn
            FROM
                bronze.core_pix
        )
        WHERE
            rn = 1
    );
    
    INSERT INTO silver.core_pix (
        transaction_id,
        transaction_date,
        transaction_month,
        cd_seqlan,
        transaction_type,
        transaction_value,
        year,
        month,
        day
    )
    SELECT * FROM temp_view_silver_core_pix;
    
    COPY (SELECT * FROM silver.core_pix)
    TO '{PATH_SILVER}/core_pix/{FILE_NAME}.parquet' (
        FORMAT PARQUET,
        OVERWRITE_OR_IGNORE true
    );
"""
comments = """
    COMMENT ON TABLE silver.core_pix IS 'Esta tabela é dedicada ao armazenamento de transações realizadas via PIX, incluindo todos os detalhes relevantes como o valor, tipo, e data da transação. Ela é crucial para o monitoramento e análise das movimentações financeiras específicas do PIX.';
    COMMENT ON COLUMN silver.core_pix.transaction_id IS 'Identificador único da transação';
    COMMENT ON COLUMN silver.core_pix.transaction_date IS 'Data de movimentação';
    COMMENT ON COLUMN silver.core_pix.transaction_month IS 'Mês-ano da movimentação';
    COMMENT ON COLUMN silver.core_pix.cd_seqlan IS 'Código sequencial de transações';
    COMMENT ON COLUMN silver.core_pix.transaction_type IS 'Tipo da transação';
    COMMENT ON COLUMN silver.core_pix.transaction_value IS 'Valor da transação';
    COMMENT ON COLUMN silver.core_pix.year IS 'Partição de Ano extraído da data de movimentação';
    COMMENT ON COLUMN silver.core_pix.month IS 'Partição de Mês extraído da data de movimentação';
    COMMENT ON COLUMN silver.core_pix.day IS 'Partição de Dia extraído da data de movimentação';
"""

duckdb_postgres_query(query)
postgres_query(comments)