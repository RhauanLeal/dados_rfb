import os
import psycopg2
from psycopg2 import pool
from dotenv import load_dotenv

load_dotenv(dotenv_path="../dados_rfb_env/.env")

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", 5432))
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_NAME = os.getenv("DB_NAME", "dados_rfb")

connection_pool = psycopg2.pool.SimpleConnectionPool(
    1,  # minconn
    20,  # maxconn
    host=DB_HOST,
    port=DB_PORT,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_NAME,
)

def get_connection():
    return connection_pool.getconn()

def return_connection(conn):
    connection_pool.putconn(conn)

def close_all_connections():
    if connection_pool:
        connection_pool.closeall()
