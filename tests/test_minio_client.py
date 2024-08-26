import pytest
import os  # Certifique-se de importar o módulo os
from pacote_ingestao.minio_client import create_bucket_if_not_exists, upload_file, download_file
from minio.error import S3Error

def test_create_bucket():
    try:
        create_bucket_if_not_exists("test-bucket")
    except S3Error:
        pytest.fail("Falha ao criar bucket no MinIO.")

def test_upload_download():
    try:
        upload_file("test-bucket", "test.txt")
        download_file("test-bucket", "test.txt", "downloaded_test.txt")
        assert os.path.exists("downloaded_test.txt"), "Falha ao baixar arquivo do MinIO"
        os.remove("downloaded_test.txt")  # Limpe o arquivo baixado após o teste
    except S3Error as e:
        pytest.fail(f"Erro ao interagir com MinIO: {e}")

def test_upload_nonexistent_file():
    with pytest.raises(FileNotFoundError):
        upload_file("test-bucket", "nonexistent_file.txt")
