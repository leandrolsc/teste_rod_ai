import os
import requests
import pandas as pd
import logging
from typing import List, Dict

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

def download_dataset(project_id: str, token: str, output_path: str) -> None:
    """
    Faz o download dos dados brutos em formato Parquet via API e salva localmente.
    """
    url = f"https://api.datamission.com.br/projects/{project_id}/dataset?format=parquet"
    headers = {"Authorization": f"Bearer {token}"}

    logging.info(f"Iniciando download dos dados para o projeto: {project_id}...")
    
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    with open(output_path, "wb") as file:
        file.write(response.content)
    
    logging.info(f"Download concluído e salvo em formato intermediário seguro: {output_path}")

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
        # Tratamento especial para permitir flexibilidade entre float/int
        if expected_type == 'numeric':
            if not pd.api.types.is_numeric_dtype(df[col]):
                raise SchemaValidationError(f"Divergência de esquema na coluna '{col}': Esperado valor numérico (int ou float), Encontrado {df[col].dtype}")
        else:
            # Tratamento normal para objetos/strings
            actual_type = str(df[col].dtype)
            if not actual_type.startswith(expected_type.strip('64').strip('32')):
                raise SchemaValidationError(f"Divergência de esquema na coluna '{col}': Esperado {expected_type}, Encontrado {actual_type}")
    
    logging.info("Validação de esquema concluída com sucesso.")

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
    Calcula o score de crédito do cliente baseado nos dados limpos.
    Inclui proteção contra divisão por zero durante a normalização.
    """
    logging.info("Iniciando cálculo de score de crédito...")
    df_scored = df.copy()

    if all(c in df_scored.columns for c in ['renda', 'divida', 'pontualidade']):
        capacidade_pgto = df_scored['renda'] - df_scored['divida']
        score_bruto = (capacidade_pgto * (df_scored['pontualidade'] / 100))
        
        # Proteção contra Divisão por Zero / NaN
        max_score = score_bruto.max()
        
        if max_score > 0:
            df_scored['score_credito'] = (score_bruto / max_score) * 1000
        else:
            # Fallback seguro caso a capacidade de pgto máxima seja <= 0
            logging.warning("Atenção: A capacidade máxima de pagamento do dataset é nula ou negativa. Atribuindo score 0.")
            df_scored['score_credito'] = 0

        # Clipping para manter dentro dos limites e conversão segura para int
        df_scored['score_credito'] = df_scored['score_credito'].clip(lower=0, upper=1000).astype(int)
    else:
        df_scored['score_credito'] = 500 

    logging.info("Cálculo de score concluído.")
    return df_scored

def export_score_to_csv(df: pd.DataFrame, output_path: str) -> None:
    """
    Exporta o dataframe final contendo as pontuações para um arquivo CSV.
    """
    logging.info(f"Exportando resultados para {output_path}...")
    df.to_csv(output_path, index=False, sep=';', encoding='utf-8')
    logging.info("Exportação concluída com sucesso.")

# ==========================================
# Execução Principal (Main)
# ==========================================

if __name__ == "__main__":
    PROJECT_ID = "99c511dd-8201-40ae-a6f6-b2f0381e172a"
    TOKEN = os.getenv("DATAMISSION_API_TOKEN", "SEU_TOKEN_AQUI_CASO_NAO_USE_ENV") 
    
    ARQUIVO_PARQUET_BRUTO = f"dataset_{PROJECT_ID}.parquet"
    ARQUIVO_CSV_FINAL = "score_credito_resultados.csv"

    # Atualizado para utilizar o helper 'numeric', evitando a restrição de float64 vs int64
    ESQUEMA_ESPERADO = {
        'id_cliente': 'object',
        'renda': 'numeric',
        'divida': 'numeric',
        'pontualidade': 'numeric' 
    }

    try:
        download_dataset(PROJECT_ID, TOKEN, ARQUIVO_PARQUET_BRUTO)
        df_bruto = pd.read_parquet(ARQUIVO_PARQUET_BRUTO)
        validate_schema(df_bruto, ESQUEMA_ESPERADO)
        df_limpo = clean_data(df_bruto)
        df_score = calculate_score(df_limpo)
        
        colunas_exportacao = ['id_cliente', 'score_credito']
        df_final = df_score[colunas_exportacao] if all(c in df_score.columns for c in colunas_exportacao) else df_score
        
        export_score_to_csv(df_final, ARQUIVO_CSV_FINAL)

    except requests.exceptions.HTTPError as http_err:
        logging.error(f"Erro na requisição à API: {http_err}")
    except MissingColumnError as mc_err:
        logging.error(f"Erro de Validação (Coluna Ausente): {mc_err}")
    except SchemaValidationError as sv_err:
        logging.error(f"Erro de Validação (Divergência de Tipo): {sv_err}")
    except Exception as e:
        logging.error(f"Erro inesperado durante o pipeline: {e}")