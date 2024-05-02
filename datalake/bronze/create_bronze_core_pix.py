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
    CREATE TABLE IF NOT EXISTS bronze.core_pix (
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
    
    CREATE OR REPLACE TEMP VIEW temp_view_bronze_core_pix AS (
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
                ROW_NUMBER() OVER (PARTITION BY id_transaction ORDER BY dt_transaction ASC) AS rn
            FROM
                raw.core_pix
        )
        WHERE
            rn = 1
    );
    
    INSERT INTO bronze.core_pix (
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
    SELECT * FROM temp_view_bronze_core_pix;
    
    COPY (SELECT * FROM bronze.core_pix)
    TO '{PATH_BRONZE}/core_pix/{FILE_NAME}.parquet' (
        FORMAT PARQUET
    );
"""
comments = """
    COMMENT ON COLUMN bronze.core_pix.transaction_id IS 'Identificador único da transação';
    COMMENT ON COLUMN bronze.core_pix.transaction_date IS 'Data de movimentação';
    COMMENT ON COLUMN bronze.core_pix.transaction_month IS 'Mês-ano da movimentação';
    COMMENT ON COLUMN bronze.core_pix.cd_seqlan IS 'Código sequencial de transações';
    COMMENT ON COLUMN bronze.core_pix.transaction_type IS 'Tipo da transação';
    COMMENT ON COLUMN bronze.core_pix.transaction_value IS 'Valor da transação';
    COMMENT ON COLUMN bronze.core_pix.year IS 'Ano extraído da data de movimentação';
    COMMENT ON COLUMN bronze.core_pix.month IS 'Mês extraído da data de movimentação';
    COMMENT ON COLUMN bronze.core_pix.day IS 'Dia extraído da data de movimentação';
"""

duckdb_postgres_query(query)
postgres_query(comments)