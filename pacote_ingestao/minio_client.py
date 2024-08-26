from minio import Minio
import os
from dotenv import load_dotenv
import logging
from io import BytesIO
from minio.error import S3Error

load_dotenv()

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

def create_bucket_if_not_exists(bucket_name):
    try:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            logger.info(f"Bucket '{bucket_name}' criado com sucesso.")
        else:
            logger.info(f"Bucket '{bucket_name}' já existe.")
    except S3Error as e:
        logger.error(f"Erro ao interagir com o MinIO: {e}")
        raise

def upload_file(bucket_name, file_path):
    try:
        file_name = os.path.basename(file_path)
        minio_client.fput_object(bucket_name, file_name, file_path)
        logger.info(f"Arquivo '{file_name}' enviado com sucesso para o bucket '{bucket_name}'.")
    except S3Error as e:
        logger.error(f"Erro ao fazer upload para o MinIO: {e}")
        raise

def download_file(bucket_name, file_name, local_file_path):
    try:
        minio_client.fget_object(bucket_name, file_name, local_file_path)
        logger.info(f"Arquivo '{file_name}' baixado com sucesso do bucket '{bucket_name}'.")
    except S3Error as e:
        logger.error(f"Erro ao fazer download do MinIO: {e}")
        raise
