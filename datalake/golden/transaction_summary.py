from dotenv import load_dotenv
load_dotenv()
import os
import sys
sys.path.append('/home/r42/caseWill')
from datetime import datetime

from utils.connections import duckdb_postgres_query, postgres_query


PATH_GOLDEN = os.getenv('MINIO_GOLDEN')
FILE_NAME = datetime.now().strftime('%Y-%m-%d')


query = f"""
    CREATE TABLE IF NOT EXISTS golden.transaction_summary (
        uf_name                     VARCHAR     NULL,
        uf                          VARCHAR     NULL,
        year                        INTEGER     NOT NULL,
        month                       INTEGER     NOT NULL,
        day                         INTEGER     NOT NULL,
        total_transaction_value     DOUBLE      NULL,
        distinct_transaction_count  INTEGER     NULL,
        distinct_user_count         INTEGER     NULL
    );

    INSERT INTO golden.transaction_summary (
        uf_name, 
        uf, 
        year, 
        month, 
        day, 
        total_transaction_value, 
        distinct_transaction_count, 
        distinct_user_count
    )
    SELECT
        uf_name,
        uf,
        year,
        month,
        day,
        SUM(transaction_value) AS total_transaction_value,
        COUNT(DISTINCT transaction_id) AS distinct_transaction_count,
        COUNT(DISTINCT customer_id) AS distinct_user_count
    FROM
        golden.all_transaction
    GROUP BY
        uf_name,
        uf,
        year,
        month,
        day;

    COPY (SELECT * FROM golden.transaction_summary)
    TO '{PATH_GOLDEN}/transaction_summary/{FILE_NAME}.parquet' (
        FORMAT PARQUET,
        OVERWRITE_OR_IGNORE true
    );
"""
comments = """
    COMMENT ON TABLE golden.transaction_summary IS 'Tabela de resumo das transações PIX, agrupadas por estado, ano, mês e dia, com métricas de valor total da transação, contagem distinta de transações e contagem distinta de usuários.';
    COMMENT ON COLUMN golden.transaction_summary.uf_name IS 'Nome do estado de residência do cliente.';
    COMMENT ON COLUMN golden.transaction_summary.uf IS 'Sigla do estado de residência do cliente.';
    COMMENT ON COLUMN golden.transaction_summary.year IS 'Partição de Ano da data da transação.';
    COMMENT ON COLUMN golden.transaction_summary.month IS 'Partição de Mês da data da transação.';
    COMMENT ON COLUMN golden.transaction_summary.day IS 'Partição de Dia da data da transação.';
    COMMENT ON COLUMN golden.transaction_summary.total_transaction_value IS 'Valor total das transações PIX agrupadas.';
    COMMENT ON COLUMN golden.transaction_summary.distinct_transaction_count IS 'Contagem distinta de transações PIX.';
    COMMENT ON COLUMN golden.transaction_summary.distinct_user_count IS 'Contagem distinta de usuários envolvidos nas transações PIX.';
"""

duckdb_postgres_query(query)
postgres_query(comments)