import requests
import zipfile
import io
import os

url = "https://download1588.mediafire.com/rlk25ccdj5gg47a3EhcUzyoPWB7UbcoWq3vShnc98EugNVQeXzsJvthCEnU1AOAM5EAviJVUAtUHDwMouZEGurtumkl4wO47d9MNIPEnF5nxLgHlNQgnK83DdhJMNL-th3kEB20yDP0CgEc2CM4APor35ZDOh0vUFsE5v5Tzu6Q/nt4ppp6zrjtsv1l/csvs.zip"

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
