import os
import requests
import pandas as pd
import logging
from typing import Dict, Tuple
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
    Faz o download dos dados brutos em formato Parquet via API antes de qualquer processamento.
    Captura metadados da resposta para identificar a versão do lote (ETag ou Timestamp).
    
    Retorna:
        str: Versão do dataset obtida no momento do download.
    """
    url = f"https://api.datamission.com.br/projects/{project_id}/dataset?format=parquet"
    headers = {"Authorization": f"Bearer {token}"}

    logging.info(f"Iniciando download dos dados para o projeto: {project_id}...")
    
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    # Tentativa de capturar a versão oficial via ETag ou Data de Modificação. 
    # Caso a API não forneça, gera um timestamp do lote.
    dataset_version = response.headers.get('ETag', response.headers.get('Last-Modified', f"batch_{datetime.now().strftime('%Y%m%d%H%M%S')}")).replace('"', '')

    with open(output_path, "wb") as file:
        file.write(response.content)
    
    logging.info(f"Download concluído (Versão do Lote: {dataset_version}). Salvo em: {output_path}")
    return dataset_version

def validate_schema(df: pd.DataFrame, expected_schema: Dict[str, str]) -> None:
    """
    Valida a presença de colunas críticas e seus tipos de dados.
    Aceita 'numeric' para validar tanto inteiros quanto floats com segurança.
    """
    logging.info("Iniciando validação de esquema de dados...")
    
    missing_cols = [col for col in expected_schema.keys() if col not in df.columns]
    if missing_cols:
        raise MissingColumnError(f"Faltam colunas críticas esperadas no dataset: {missing_cols}")

    for col, expected_type in expected_schema.items():
        if expected_type == 'numeric':
            if not pd.api.types.is_numeric_dtype(df[col]):
                raise SchemaValidationError(f"Divergência de esquema na coluna '{col}': Esperado valor numérico, Encontrado {df[col].dtype}")
        else:
            actual_type = str(df[col].dtype)
            if not actual_type.startswith(expected_type.strip('64').strip('32')):
                raise SchemaValidationError(f"Divergência de esquema na coluna '{col}': Esperado {expected_type}, Encontrado {actual_type}")
    
    logging.info("Validação de esquema concluída com sucesso.")

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica regras de limpeza focadas na detecção de anomalias.
    
    Passos realizados:
    - Remoção de registros duplicados (idênticos).
    - Preenchimento de valores faltantes (NaN) com 0 para métricas de risco.
    """
    logging.info("Iniciando limpeza de dados (duplicatas e nulos)...")
    df_clean = df.copy()
    
    linhas_antes = len(df_clean)
    df_clean = df_clean.drop_duplicates(subset=['id_cliente'])
    linhas_depois = len(df_clean)
    if linhas_antes != linhas_depois:
        logging.info(f"Removidas {linhas_antes - linhas_depois} linhas duplicadas.")

    colunas_metricas = ['frequencia_transacoes', 'volume_relativo', 'percentual_estornos']
    colunas_presentes = [c for c in colunas_metricas if c in df_clean.columns]
    
    df_clean[colunas_presentes] = df_clean[colunas_presentes].fillna(0)
        
    logging.info("Limpeza concluída.")
    return df_clean

def calculate_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula o Score de Anomalia combinando frequência, volume e estornos.
    Normaliza de 0 a 100 com proteção contra divisão por zero.
    """
    logging.info("Iniciando cálculo do score de anomalias...")
    df_scored = df.copy()

    PESO_FREQ, PESO_VOL, PESO_ESTORNO = 0.2, 0.3, 0.5

    score_bruto = (
        (df_scored['frequencia_transacoes'] * PESO_FREQ) + 
        (df_scored['volume_relativo'] * PESO_VOL) + 
        (df_scored['percentual_estornos'] * PESO_ESTORNO)
    )
    
    max_score = score_bruto.max()
    if max_score > 0:
        df_scored['score_anomalia'] = (score_bruto / max_score) * 100
    else:
        logging.warning("Métricas nulas. Atribuindo score de anomalia 0.")
        df_scored['score_anomalia'] = 0

    df_scored['score_anomalia'] = df_scored['score_anomalia'].clip(lower=0, upper=100).round(2)
    logging.info("Cálculo de anomalias concluído.")
    return df_scored

def export_score_to_csv(df: pd.DataFrame, version_metadata: str, output_path: str) -> None:
    """
    Exporta o dataframe final para CSV e insere os metadados de versão da API.
    """
    logging.info(f"Exportando resultados com metadados da versão ({version_metadata})...")
    
    # Adicionando a coluna de metadados
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
    
    ARQUIVO_PARQUET_BRUTO = f"dataset_anomalias_{PROJECT_ID}.parquet"
    ARQUIVO_CSV_FINAL = "score_anomalias_resultados.csv"

    ESQUEMA_ESPERADO = {
        'id_cliente': 'object',
        'frequencia_transacoes': 'numeric',
        'volume_relativo': 'numeric',
        'percentual_estornos': 'numeric'
    }

    try:
        versao_dataset = download_dataset(PROJECT_ID, TOKEN, ARQUIVO_PARQUET_BRUTO)
        
        df_bruto = pd.read_parquet(ARQUIVO_PARQUET_BRUTO)
        validate_schema(df_bruto, ESQUEMA_ESPERADO)
        
        df_limpo = clean_data(df_bruto)
        df_score = calculate_score(df_limpo)
        
        colunas_exportacao = ['id_cliente', 'score_anomalia']
        df_final = df_score[colunas_exportacao]
        
        export_score_to_csv(df_final, versao_dataset, ARQUIVO_CSV_FINAL)

    except requests.exceptions.HTTPError as http_err:
        logging.error(f"Erro na requisição à API: {http_err}")
    except MissingColumnError as mc_err:
        logging.error(f"Erro de Validação (Coluna Ausente): {mc_err}")
    except SchemaValidationError as sv_err:
        logging.error(f"Erro de Validação (Divergência de Tipo): {sv_err}")
    except Exception as e:
        logging.error(f"Erro inesperado durante o pipeline: {e}")