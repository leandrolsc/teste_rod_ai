import requests
import datetime

def fetch_project_dataset(project_id: str, file_format: str = "parquet", token: str = None):
    """
    Consome dados da API DataMission de forma desacoplada.
    
    Args:
        project_id (str): Identificador único do projeto (UUID).
        file_format (str): Formato desejado (ex: 'parquet', 'json'). Default: 'parquet'.
        token (str): Token de autenticação Bearer.
        
    Returns:
        bytes: Conteúdo bruto do arquivo baixado.
    """
    url = f"https://api.datamission.com.br/projects/{project_id}/dataset"
    params = {"format": file_format}
    headers = {"Authorization": f"Bearer {token}"}

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.content