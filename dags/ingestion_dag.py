from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import os
import logging

# Importando as funções modulares de transformação
from src.transformations import realizar_parse_json, validar_e_limpar_dados, salvar_como_parquet

# Configurações do Projeto
PROJECT_ID = "projeto_xyz_123"
# URL mockada para fins de demonstração. Em produção, usar Variables do Airflow
API_URL = f"https://api.exemplo.com/v1/projects/{PROJECT_ID}/dataset" 

# Caminhos locais/temporários
TEMP_DIR = "/tmp/data_pipeline"
RAW_JSON_PATH = f"{TEMP_DIR}/raw_dataset_{PROJECT_ID}.json"
PROCESSED_PARQUET_PATH = f"{TEMP_DIR}/processed_dataset_{PROJECT_ID}.parquet"

# Argumentos padrão da DAG
default_args = {
    'owner': 'engenharia_de_dados',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def _extrair_dados_api(**kwargs):
    """
    Faz a chamada GET na API para baixar os dados crus e salva em JSON.
    """
    os.makedirs(TEMP_DIR, exist_ok=True)
    logging.info(f"Iniciando extração da URL: {API_URL}")
    
    # Executa o GET /projects/{project_id}/dataset
    response = requests.get(API_URL, timeout=10)
    
    # Verifica se a requisição foi bem sucedida
    response.raise_for_status()
    payload = response.json()
    
    # Salvar payload retornado em storage local/temporário (Formato JSON)
    with open(RAW_JSON_PATH, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=4)
        
    logging.info(f"Dados crus salvos com sucesso em: {RAW_JSON_PATH}")

def _transformar_e_salvar_dados(**kwargs):
    """
    Lê o JSON salvo, aplica as transformações e salva no formato suportado Parquet.
    """
    # 1. Parse do JSON para Pandas
    df_raw = realizar_parse_json(RAW_JSON_PATH)
    
    # 2. Aplicação das validações (reutilizáveis e testáveis)
    df_clean = validar_e_limpar_dados(df_raw)
    
    # 3. Salvar no formato suportado (Parquet)
    salvar_como_parquet(df_clean, PROCESSED_PARQUET_PATH)

# Definição da DAG
with DAG(
    'ingestao_dados_projeto',
    default_args=default_args,
    description='DAG para ingerir dados de projeto da API e salvar em Parquet',
    schedule_interval=timedelta(days=1), # Executa diariamente
    catchup=False,
    tags=['ingestao', 'api', 'projeto'],
) as dag:

    task_extrair = PythonOperator(
        task_id='extrair_dados_api',
        python_callable=_extrair_dados_api,
        provide_context=True
    )

    task_transformar = PythonOperator(
        task_id='transformar_e_salvar_dados',
        python_callable=_transformar_e_salvar_dados,
        provide_context=True
    )

    # Definição da ordem de execução (Dependência)
    task_extrair >> task_transformar
