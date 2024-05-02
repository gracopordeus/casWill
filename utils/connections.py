import duckdb
import boto3
from botocore.client import Config
import psycopg2


import os
from dotenv import load_dotenv

load_dotenv()
POSTGRES_URI=os.getenv('POSTGRES_URI')
POSTGRES_HOST=os.getenv('POSTGRES_HOST')
POSTGRES_PORT=os.getenv('POSTGRES_PORT')
POSTGRES_DATABASE=os.getenv('POSTGRES_DATABASE')
POSTGRES_USER=os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD=os.getenv('POSTGRES_PASSWORD')

MINIO_ENDPOINT=os.getenv('MINIO_ENDPOINT')
MINIO_ENDPOINT_BASE=os.getenv('MINIO_ENDPOINT_BASE')
MINIO_KEY_ID=os.getenv('MINIO_KEY_ID')
MINIO_ACCESS_KEY=os.getenv('MINIO_ACCESS_KEY')

# Minio connections 
def minio_connection_resource():
    return boto3.resource(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_KEY_ID,
        aws_secret_access_key=MINIO_ACCESS_KEY,
        config=Config(signature_version='s3v4'),
    )

def minio_connection_client():
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_KEY_ID,
        aws_secret_access_key=MINIO_ACCESS_KEY,
        config=Config(signature_version='s3v4'),
    )




# DuckDB postgres connection
def duckdb_load_postgres():
    db = duckdb.connect()
    
    db.sql(f"""
        INSTALL iceberg;
        LOAD iceberg;
    """
    )
    
    db.sql(f"""
        INSTALL postgres;
        LOAD postgres;
        ATTACH 'dbname={POSTGRES_DATABASE} user={POSTGRES_USER} password={POSTGRES_PASSWORD} host={POSTGRES_HOST}' AS postgres (TYPE POSTGRES);
    """
    )
    
    db.sql(f"""
        INSTALL httpfs;
        LOAD httpfs;
        DROP SECRET IF EXISTS minion_storage;
        CREATE SECRET minion_storage (
            TYPE S3,
            URL_STYLE 'path',
            ENDPOINT '{MINIO_ENDPOINT_BASE}',
            KEY_ID '{MINIO_KEY_ID}',
            SECRET '{MINIO_ACCESS_KEY}'
        )
    """
    )
    
    return db

db = duckdb_load_postgres()

def duckdb_postgres_query(query:str, db=db):
    return db.sql("USE postgres; "+ query)


# Postgres directly connection
postgres_params = {
    'database': POSTGRES_DATABASE,
    'user': POSTGRES_USER,
    'password': POSTGRES_PASSWORD,
    'host': POSTGRES_HOST,
    'port': POSTGRES_PORT
}

def postgres_query(query:str, params=postgres_params):
    conn = psycopg2.connect(**params)
    cursor = conn.cursor()
    cursor.execute(query)
    print("Query executed successfully.")
    cursor.close()
    conn.close()