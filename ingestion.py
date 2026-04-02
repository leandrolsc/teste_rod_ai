import requests
import pandas as pd
import io
import logging
from datetime import datetime

# Configuração básica de log para auditoria
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_project_dataset(project_id: str, token: str, file_format: str = "csv"):
    """
    Consome a API, converte para DataFrame e registra metadados de execução.
    """
    url = f"https://api.datamission.com.br/projects/{project_id}/dataset"
    params = {"format": file_format}
    headers = {"Authorization": f"Bearer {token}"}

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        
        # Auditoria e Metadados
        content_size = len(response.content)
        logging.info(f"Ingestão concluída: Projeto={project_id}, Formato={file_format}, Tamanho={content_size} bytes")

        # Converte o conteúdo baixado em DataFrame
        df = pd.read_csv(io.BytesIO(response.content))
        return df

    except Exception as e:
        logging.error(f"Erro na ingestão: {e}")
        raise