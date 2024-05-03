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
    CREATE TABLE IF NOT EXISTS golden.all_transaction (
        transaction_id          VARCHAR     PRIMARY KEY,
        transaction_date        DATE        NULL,
        transaction_month       INTEGER     NULL,
        customer_id             INTEGER     NULL,
        cd_seqlan               INTEGER     NULL,
        transaction_type        VARCHAR     NULL,
        transaction_value       DOUBLE      NULL,
        birth_date              DATE        NULL,
        uf_name                 VARCHAR     NULL,
        uf                      VARCHAR     NULL,
        year                    INTEGER     NOT NULL,
        month                   INTEGER     NOT NULL,
        day                     INTEGER     NOT NULL
    );

    CREATE OR REPLACE TEMP VIEW temp_view_golden_all_transaction AS (
        SELECT
            pt.transaction_id,
            pt.transaction_date,
            pt.transaction_month,
            pt.customer_id,
            pt.cd_seqlan,
            pt.transaction_type,
            pt.transaction_value,
            c.birth_date,
            c.uf_name,
            c.uf,
            pt.year,
            pt.month,
            pt.day
        FROM
            silver.pix_transaction pt
        JOIN
            silver.customer c
        ON
            pt.customer_id = c.customer_id
    );

    INSERT INTO golden.all_transaction
    SELECT *
    FROM temp_view_golden_all_transaction;

    COPY (SELECT * FROM golden.all_transaction)
    TO '{PATH_GOLDEN}/all_transaction/{FILE_NAME}.parquet' (
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