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

    COPY (SELECT * FROM golden.all_transaction)
    TO '{PATH_GOLDEN}/transaction_summary/{FILE_NAME}.parquet' (
        FORMAT PARQUET,
        OVERWRITE_OR_IGNORE true
    );
"""
comments = """
    COMMENT ON TABLE golden.all_transaction IS 'Tabela abrangente que junta informações de transações PIX com dados detalhados dos clientes para fornecer um panorama detalhado das atividades financeiras dos clientes relacionadas ao PIX.';
    COMMENT ON COLUMN golden.all_transaction.transaction_id IS 'Identificador único da transação PIX, combinando informações das tabelas core_pix e core_account.';
    COMMENT ON COLUMN golden.all_transaction.transaction_date IS 'Data da transação conforme registrada na tabela core_account.';
    COMMENT ON COLUMN golden.all_transaction.transaction_month IS 'Mês numérico da transação conforme registrada na tabela core_account.';
    COMMENT ON COLUMN golden.all_transaction.customer_id IS 'Identificador único do cliente associado à transação.';
    COMMENT ON COLUMN golden.all_transaction.cd_seqlan IS 'Código sequencial do lançamento da transação.';
    COMMENT ON COLUMN golden.all_transaction.transaction_type IS 'Tipo da transação, especificando se é PIX ou outro tipo.';
    COMMENT ON COLUMN golden.all_transaction.transaction_value IS 'Valor monetário da transação conforme registrado na tabela core_account.';
    COMMENT ON COLUMN golden.all_transaction.birth_date IS 'Data de nascimento do cliente.';
    COMMENT ON COLUMN golden.all_transaction.uf_name IS 'Nome do estado de residência do cliente.';
    COMMENT ON COLUMN golden.all_transaction.uf IS 'Sigla do estado de residência do cliente.';
    COMMENT ON COLUMN golden.all_transaction.year IS 'Partição de Ano extraído da data da transação na tabela core_account.';
    COMMENT ON COLUMN golden.all_transaction.month IS 'Partição de Mês extraído da data da transação na tabela core_account.';
    COMMENT ON COLUMN golden.all_transaction.day IS 'Partição de Dia extraído da data da transação na tabela core_account.';
"""

duckdb_postgres_query(query)
postgres_query(comments)