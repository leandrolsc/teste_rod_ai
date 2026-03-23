import requests
import logging

logger = logging.getLogger(__name__)

def fetch_traffic_logs(base_url: str, project_id: str) -> list:
    """
    Faz a chamada GET ao endpoint de logs de tráfego.
    """
    url = f"{base_url}/projects/{project_id}/dataset?format=log"
    logger.info(f"Iniciando requisição GET para {url}")
    
    try:
        # Timeout definido para evitar travamentos
        response = requests.get(url, timeout=30)
        
        # Levanta exceção para códigos de erro HTTP (4xx, 5xx)
        response.raise_for_status()
        
        logger.info("Dados de tráfego e conversão baixados com sucesso.")
        
        # Assumindo que o endpoint retorna um JSON estruturado (lista de dicionários)
        return response.json()
        
    except requests.exceptions.HTTPError as http_err:
        logger.error(f"Erro HTTP ao buscar dados da API: {http_err}")
        raise
    except requests.exceptions.RequestException as req_err:
        logger.error(f"Erro de conexão/requisição ao buscar dados: {req_err}")
        raise