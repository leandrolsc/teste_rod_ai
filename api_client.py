import requests
import logging

# Configuração básica de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatasetClient:
    """
    Cliente para interagir com a API de Datasets do projeto.
    """
    
    def __init__(self, base_url, api_token):
        self.base_url = base_url.rstrip('/')
        self.headers = {
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json"
        }

    def get_dataset(self, project_id):
        """
        Chama o endpoint /api/v1/projects/{project_id}/dataset e trata a resposta.
        """
        url = f"{self.base_url}/api/v1/projects/{project_id}/dataset"
        
        try:
            logger.info(f"Iniciando download do dataset para o projeto: {project_id}")
            response = requests.get(url, headers=self.headers, timeout=30)
            
            # Validação imediata do status HTTP
            response.raise_for_status()
            
            # Validação do formato de conteúdo
            data = response.json()
            if not isinstance(data, list):
                raise ValueError("O formato do dataset recebido é inválido (esperado uma lista).")
                
            return data

        except requests.exceptions.HTTPError as http_err:
            logger.error(f"Erro HTTP ao acessar a API: {http_err}")
            raise
        except requests.exceptions.RequestException as err:
            logger.error(f"Erro de conexão: {err}")
            raise
        except ValueError as json_err:
            logger.error(f"Erro ao processar JSON: {json_err}")
            raise
