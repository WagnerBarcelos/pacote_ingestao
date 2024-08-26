import pytest
import pandas as pd
from pacote_ingestao.data_processing import process_data, prepare_dataframe_for_insert
import os

def test_process_data():
    data = {'date': 1692345600, 'dados': 12345}
    filename = process_data(data)
    assert os.path.exists(filename), "Arquivo Parquet não foi criado"
    os.remove(filename)

def test_prepare_dataframe_for_insert():
    data = {'date': [1692345600], 'dados': [12345]}
    df = pd.DataFrame(data)
    df_prepared = prepare_dataframe_for_insert(df)
    assert 'data_ingestao' in df_prepared.columns, "Coluna 'data_ingestao' ausente"
    assert 'dado_linha' in df_prepared.columns, "Coluna 'dado_linha' ausente"
    assert 'tag' in df_prepared.columns, "Coluna 'tag' ausente"
