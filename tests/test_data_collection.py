import sys
import os
import io
import zipfile
import pytest
from unittest import mock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
from data_collection import download_and_unzip

@mock.patch("data_collection.requests.get")
def test_download_and_unzip_success(mock_get, tmp_path):
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, mode="w") as zf:
        zf.writestr("teste.txt", "conteúdo de teste")
    zip_buffer.seek(0)

    mock_get.return_value.status_code = 200
    mock_get.return_value.content = zip_buffer.read()

    download_and_unzip("http://fake-url.com", extract_to=tmp_path)

    extracted_file = tmp_path / "teste.txt"
    assert extracted_file.exists()
    assert extracted_file.read_text() == "conteúdo de teste"

@mock.patch("data_collection.requests.get")
def test_download_and_unzip_http_error(mock_get, capsys):
    mock_get.return_value.status_code = 404

    download_and_unzip("http://fake-url.com", extract_to="fake_dir")

    captured = capsys.readouterr()
    assert "Erro ao baixar o arquivo" in captured.out

@mock.patch("data_collection.requests.get")
def test_download_and_unzip_exception(mock_get, capsys):
    mock_get.return_value.status_code = 200
    mock_get.return_value.content = b"not-a-zip"

    download_and_unzip("http://fake-url.com", extract_to="fake_dir")

    captured = capsys.readouterr()
    assert "Erro ao concluir o processo" in captured.out
