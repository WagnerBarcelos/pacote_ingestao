import clickhouse_connect
import os
from dotenv import load_dotenv
import logging

load_dotenv()

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST')
CLICKHOUSE_PORT = os.getenv('CLICKHOUSE_PORT')

def get_client():
    try:
        client = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
        logger.info("Conexão com ClickHouse estabelecida.")
        return client
    except Exception as e:
        logger.error(f"Erro ao conectar ao ClickHouse: {e}")
        raise

def execute_sql_script(script_path):
    client = get_client()
    try:
        with open(script_path, 'r') as file:
            sql_script = file.read()
        client.command(sql_script)
        logger.info("Script SQL executado com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao executar script SQL: {e}")
        raise

def insert_dataframe(client, table_name, df):
    try:
        client.insert_df(table_name, df)
        logger.info(f"Dados inseridos com sucesso na tabela {table_name}.")
    except Exception as e:
        logger.error(f"Erro ao inserir dados no ClickHouse: {e}")
        raise
