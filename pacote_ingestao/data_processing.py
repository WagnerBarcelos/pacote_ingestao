import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_data(data):
    try:
        df = pd.DataFrame([data])
        filename = f"raw_data_{datetime.now().strftime('%Y%m%d%H%M%S')}.parquet"
        table = pa.Table.from_pandas(df)
        pq.write_table(table, filename)
        logger.info(f"Arquivo Parquet '{filename}' criado com sucesso.")
        return filename
    except Exception as e:
        logger.error(f"Erro ao processar dados: {e}")
        raise

def prepare_dataframe_for_insert(df):
    try:
        df['data_ingestao'] = datetime.now()
        df['dado_linha'] = df.apply(lambda row: row.to_json(), axis=1)
        df['tag'] = 'example_tag'
        logger.info("DataFrame preparado para inserção.")
        return df[['data_ingestao', 'dado_linha', 'tag']]
    except Exception as e:
        logger.error(f"Erro ao preparar DataFrame: {e}")
        raise
