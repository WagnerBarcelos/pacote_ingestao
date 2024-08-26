from datetime import datetime
import pandas as pd
import pytest
from pacote_ingestao.clickhouse_client import get_client, insert_dataframe
from pacote_ingestao.clickhouse_client import execute_sql_script

def test_clickhouse_connection():
    client = get_client()
    assert client is not None, "Conexão com ClickHouse falhou"

def test_sql_execution():
    try:
        execute_sql_script('sql/create_table.sql')
    except Exception as e:
        pytest.fail(f"Falha ao executar script SQL: {e}")

def test_insert_dataframe():
    client = get_client()
    data = {
        'data_ingestao': [datetime(2023, 1, 1, 0, 0, 0)],  # Use datetime em vez de string
        'dado_linha': ['{"date": 1692345600, "dados": 12345}'],
        'tag': ['example_tag']
    }
    df = pd.DataFrame(data)
    try:
        insert_dataframe(client, 'working_data', df)
    except Exception as e:
        pytest.fail(f"Erro ao inserir dataframe: {e}")