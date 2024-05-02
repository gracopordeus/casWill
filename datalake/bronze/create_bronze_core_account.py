from dotenv import load_dotenv
load_dotenv()
import os
import sys
sys.path.append('/home/r42/caseWill')

from utils.connections import duckdb_postgres_query


PATH_RAW = os.getenv('MINIO_RAW')

query = f"""
    CREATE TABLE IF NOT EXISTS raw.core_account (
        dt_transaction          DATE        NULL,
        dt_month                INTEGER     NULL,
        surrogate_key           INTEGER     NULL,
        cd_seqlan               INTEGER     NULL,
        ds_transaction_type     VARCHAR     NULL,
        vl_transaction          DOUBLE      NULL,
        id_transaction          VARCHAR     NULL
    );
    
    WITH df AS (
        SELECT 
            *
        FROM read_csv_auto('{PATH_RAW}/core_account.csv')
    )
    INSERT INTO raw.core_account (
        dt_transaction,
        dt_month,
        surrogate_key,
        cd_seqlan,
        ds_transaction_type,
        vl_transaction,
        id_transaction
    )
    SELECT * FROM df
"""

duckdb_postgres_query(query)