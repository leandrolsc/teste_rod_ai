import os
import requests
import pandas as pd
import logging
from typing import Dict
from datetime import datetime

# Configuração básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ==========================================
# Exceções Customizadas
# ==========================================

class MissingColumnError(Exception):
    """Exceção lançada quando o dataset não contém as colunas mínimas necessárias."""
    pass

class SchemaValidationError(Exception):
    """Exceção lançada quando os tipos de dados das colunas divergem do esperado."""
    pass

# ==========================================
# Funções do Pipeline
# ==========================================

def download_dataset(project_id: str, token: str, output_path: str) -> str:
    """
    Faz o download dos dados brutos de crédito em formato Parquet via API.
    Captura os metadados do cabeçalho da API para controle de versão do lote.
    
    Retorna:
        str: Versão do dataset (ETag, Last-Modified ou Timestamp gerado).
    """
    url = f"https://api.datamission.com.br/projects/{project_id}/dataset?format=parquet"
    headers = {"Authorization": f"Bearer {token}"}

    logging.info(f"Iniciando download dos dados para o projeto: {project_id}...")
    
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    # Captura a versão do lote fornecida pela API. Se indisponível, cria um timestamp.
    dataset_version = response.headers.get(
        'ETag', 
        response.headers.get('Last-Modified', f"batch_{datetime.now().strftime('%Y%m%d%H%M%S')}")
    ).replace('"', '')

    with open(output_path, "wb") as file:
        file.write(response.content)
    
    logging.info(f"Download concluído (Versão do Lote: {dataset_version}). Salvo em: {output_path}")
    return dataset_version

def validate_schema(df: pd.DataFrame, expected_schema: Dict[str, str]) -> None:
    """
    Valida a presença de colunas críticas e seus tipos de dados.
    """
    logging.info("Iniciando validação de esquema de dados...")
    
    missing_cols = [col for col in expected_schema.keys() if col not in df.columns]
    if missing_cols:
        raise MissingColumnError(f"Faltam colunas críticas esperadas no dataset: {missing_cols}")

    for col, expected_type in expected_schema.items():
        if expected_type == 'numeric':
            if not pd.api.types.is_numeric_dtype(df[col]):
                raise SchemaValidationError(f"Divergência de esquema na coluna '{col}': Esperado numérico, Encontrado {df[col].dtype}")
        else:
            actual_type = str(df[col].dtype)
            if not actual_type.startswith(expected_type.strip('64').strip('32')):
                raise SchemaValidationError(f"Divergência de esquema na coluna '{col}': Esperado {expected_type}, Encontrado {actual_type}")
    
    logging.info("Validação de esquema concluída.")

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza a limpeza básica dos dados de crédito antes do processamento.
    """
    logging.info("Iniciando limpeza de dados...")
    df_clean = df.copy()
    
    colunas_calculo = ['renda', 'divida', 'pontualidade']
    colunas_presentes = [c for c in colunas_calculo if c in df_clean.columns]
    
    df_clean = df_clean.dropna(subset=colunas_presentes)

    if 'renda' in df_clean.columns:
        df_clean = df_clean[df_clean['renda'] >= 0]
        
    logging.info(f"Limpeza concluída. Linhas remanescentes: {len(df_clean)}")
    return df_clean

def calculate_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula o score de crédito do cliente normalizado (0 a 1000).
    Inclui proteção contra divisão por zero para lotes com capacidade nula.
    """
    logging.info("Iniciando cálculo de score de crédito...")
    df_scored = df.copy()

    capacidade_pgto = df_scored['renda'] - df_scored['divida']
    score_bruto = (capacidade_pgto * (df_scored['pontualidade'] / 100))
    
    max_score = score_bruto.max()
    
    if max_score > 0:
        df_scored['score_credito'] = (score_bruto / max_score) * 1000
    else:
        logging.warning("Capacidade de pagamento máxima <= 0. Atribuindo score seguro (0).")
        df_scored['score_credito'] = 0

    df_scored['score_credito'] = df_scored['score_credito'].clip(lower=0, upper=1000).astype(int)

    logging.info("Cálculo de score concluído.")
    return df_scored

def export_score_to_csv(df: pd.DataFrame, version_metadata: str, output_path: str) -> None:
    """
    Exporta o dataframe final para CSV anexando a versão do dataset da API para rastreabilidade.
    """
    logging.info(f"Exportando resultados com metadados do lote ({version_metadata})...")
    
    df_export = df.copy()
    df_export['versao_lote_api'] = version_metadata
    
    df_export.to_csv(output_path, index=False, sep=';', encoding='utf-8')
    logging.info("Exportação concluída com sucesso.")

# ==========================================
# Execução Principal (Main)
# ==========================================

if __name__ == "__main__":
    PROJECT_ID = "99c511dd-8201-40ae-a6f6-b2f0381e172a"
    TOKEN = os.getenv("DATAMISSION_API_TOKEN", "SEU_TOKEN_AQUI_CASO_NAO_USE_ENV") 
    
    ARQUIVO_PARQUET_BRUTO = f"dataset_{PROJECT_ID}.parquet"
    ARQUIVO_CSV_FINAL = "score_credito_resultados.csv"

    ESQUEMA_ESPERADO = {
        'id_cliente': 'object',
        'renda': 'numeric',
        'divida': 'numeric',
        'pontualidade': 'numeric' 
    }

    try:
        # Extrai os dados E a versão (ETag/Timestamp) do lote
        versao_dataset = download_dataset(PROJECT_ID, TOKEN, ARQUIVO_PARQUET_BRUTO)
        
        df_bruto = pd.read_parquet(ARQUIVO_PARQUET_BRUTO)
        validate_schema(df_bruto, ESQUEMA_ESPERADO)
        df_limpo = clean_data(df_bruto)
        df_score = calculate_score(df_limpo)
        
        colunas_exportacao = ['id_cliente', 'score_credito']
        df_final = df_score[colunas_exportacao] if all(c in df_score.columns for c in colunas_exportacao) else df_score
        
        # Passa a versão do lote para a exportação
        export_score_to_csv(df_final, versao_dataset, ARQUIVO_CSV_FINAL)

    except requests.exceptions.HTTPError as http_err:
        logging.error(f"Erro na requisição à API: {http_err}")
    except MissingColumnError as mc_err:
        logging.error(f"Erro de Validação (Coluna Ausente): {mc_err}")
    except SchemaValidationError as sv_err:
        logging.error(f"Erro de Validação (Divergência de Tipo): {sv_err}")
    except Exception as e:
        logging.error(f"Erro inesperado durante o pipeline: {e}")