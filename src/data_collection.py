import requests
import zipfile
import io
import os

url = "https://uesli.blob.core.windows.net/uesli/csvs.zip"

def download_and_unzip(url, extract_to="."):
    response = requests.get(url)
    try:
        if response.status_code == 200:
            with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
                zip_ref.extractall(extract_to)
            print("Processo concluído")
        else:
            print("Erro ao baixar o arquivo: Status code {response.status_code}")
    except Exception as e:
        print("Erro ao concluir o processo: ", e)
    
download_and_unzip(url, extract_to="./data/raw")
