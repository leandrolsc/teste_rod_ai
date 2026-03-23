import os

# Configurações gerais da API e do projeto
PROJECT_ID = os.getenv("PROJECT_ID", "exemplo_projeto_123")
API_BASE_URL = os.getenv("API_BASE_URL", "https://api.exemplo.com")

# Diretório onde os arquivos CSV e JSON serão salvos
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "./data")

# Intervalo da rotina em minutos
POLL_INTERVAL_MINUTES = int(os.getenv("POLL_INTERVAL_MINUTES", "60"))