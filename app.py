from flask import Flask, request, jsonify
from pacote_ingestao.minio_client import create_bucket_if_not_exists, upload_file, download_file
from pacote_ingestao.clickhouse_client import execute_sql_script, get_client, insert_dataframe
from pacote_ingestao.data_processing import process_data, prepare_dataframe_for_insert
import pandas as pd
import logging

app = Flask(__name__)

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

create_bucket_if_not_exists("raw-data")
execute_sql_script('sql/create_table.sql')

@app.route('/data', methods=['POST'])
def receive_data():
    data = request.get_json()
    if not data or 'date' not in data or 'dados' not in data:
        logger.warning("Formato de dados inválido recebido.")
        return jsonify({"error": "Formato de dados inválido"}), 400

    try:
        # Processamento e upload de dados
        filename = process_data(data)
        upload_file("raw-data", filename)
        download_file("raw-data", filename, f"downloaded_{filename}")
        
        # Leitura e inserção de dados no ClickHouse
        df_parquet = pd.read_parquet(f"downloaded_{filename}")
        df_prepared = prepare_dataframe_for_insert(df_parquet)
        client = get_client()
        insert_dataframe(client, 'working_data', df_prepared)
        
        logger.info("Dados recebidos e processados com sucesso.")
        return jsonify({"message": "Dados recebidos, armazenados e processados com sucesso"}), 200

    except Exception as e:
        logger.error(f"Erro ao processar solicitação: {e}")
        return jsonify({"error": "Erro interno do servidor"}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
