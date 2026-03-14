import requests
import logging
import tempfile
import os
from pathlib import Path

# Configuração básica de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatasetClient:
    """
    Cliente para interagir com a API de Datasets com suporte a múltiplos formatos
    e persistência temporária de arquivos.
    """
    
    SUPPORTED_FORMATS = ['parquet', 'json', 'csv', 'log', 'xml']

    def __init__(self, base_url, api_token, default_format='parquet'):
        self.base_url = base_url.rstrip('/')
        self.headers = {
            "Authorization": f"Bearer {api_token}",
            "Accept": "application/octet-stream"
        }
        self.default_format = default_format

    def get_dataset(self, project_id, file_format=None):
        """
        Baixa o dataset, persiste em arquivo temporário e retorna o caminho.
        Lógica: Prioriza o formato passado, fallback para o default_format.
        """
        fmt = file_format if file_format in self.SUPPORTED_FORMATS else self.default_format
        url = f"{self.base_url}/api/v1/projects/{project_id}/dataset"
        params = {"format": fmt}
        
        try:
            logger.info(f"Solicitando dataset ({fmt}) para o projeto: {project_id}")
            response = requests.get(url, headers=self.headers, params=params, timeout=30, stream=True)
            
            # Validação da resposta HTTP
            response.raise_for_status()

            # Persistência temporária (Requisito Major)
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=f".{fmt}")
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    temp_file.write(chunk)
            temp_file.close()
            
            logger.info(f"Dataset persistido temporariamente em: {temp_file.name}")
            return temp_file.name

        except requests.exceptions.HTTPError as http_err:
            logger.error(f"Erro HTTP ({response.status_code}): {http_err}")
            raise
        except Exception as err:
            logger.error(f"Erro inesperado no download: {err}")
            raise

    def load_data_from_temp(self, file_path):
        """
        Lógica simples para decidir como abrir o arquivo baseado na extensão.
        """
        ext = Path(file_path).suffix.lower()
        logger.info(f"Lendo dados do arquivo temporário: {file_path}")
        
        # Exemplo simplificado de carga (JSON)
        if ext == '.json':
            import json
            with open(file_path, 'r') as f:
                return json.load(f)
        
        # Para outros formatos (Parquet/CSV), retornaria buffers ou DataFrames
        return file_path
