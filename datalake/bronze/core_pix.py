from dotenv import load_dotenv
load_dotenv()
import os
import sys
sys.path.append('/home/r42/caseWill')

from utils.connections import duckdb_postgres_query


PATH_BRONZE = os.getenv('MINIO_BRONZE')

query = f"""
    CREATE TABLE IF NOT EXISTS bronze.core_pix (
        dt_transaction          DATE        NULL,
        dt_month                INTEGER     NULL,
        cd_seqlan               INTEGER     NULL,
        ds_transaction_type     VARCHAR     NULL,
        vl_transaction          DOUBLE      NULL,
        id_transaction          VARCHAR     NULL
    );
    
    WITH df AS (
        SELECT 
            *
        FROM read_csv_auto('{PATH_BRONZE}/core_pix.csv')
    )
    INSERT INTO bronze.core_pix (
        dt_transaction,
        dt_month,
        cd_seqlan,
        ds_transaction_type,
        vl_transaction,
        id_transaction
    )
    SELECT * FROM df
"""

duckdb_postgres_query(query)