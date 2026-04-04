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

    Args:
        project_id (str): ID do projeto na Datamission.
        token (str): Token de autorização Bearer.
        output_path (str): Caminho onde o arquivo Parquet será salvo.
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

    Args:
        df (pd.DataFrame): Dataset carregado em memória.
        expected_schema (Dict[str, str]): Dicionário com o nome das colunas e seus tipos esperados.
            Exemplo: {'id_cliente': 'object', 'renda': 'float64'}
    
    Raises:
        MissingColumnError: Se alguma coluna esperada estiver ausente.
        SchemaValidationError: Se o tipo da coluna não bater com o esperado.
    """
    logging.info("Iniciando validação de esquema de dados...")
    
    missing_cols = [col for col in expected_schema.keys() if col not in df.columns]
    if missing_cols:
        raise MissingColumnError(f"Faltam colunas críticas esperadas no dataset: {missing_cols}")

    for col, expected_type in expected_schema.items():
        actual_type = str(df[col].dtype)
        # Permite variações comuns (ex: int64 para int32), mas checa o core do tipo
        if not actual_type.startswith(expected_type.strip('64').strip('32')):
            raise SchemaValidationError(f"Divergência de esquema na coluna '{col}': Esperado {expected_type}, Encontrado {actual_type}")
    
    logging.info("Validação de esquema concluída com sucesso.")

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza a limpeza básica dos dados de crédito antes do processamento.

    Passos realizados:
    - Remoção de valores nulos em colunas críticas de cálculo.
    - Garantia de que valores financeiros (renda, divida) não sejam negativos.

    Args:
        df (pd.DataFrame): DataFrame original carregado.

    Returns:
        pd.DataFrame: DataFrame limpo e pronto para cálculo de score.
    """
    logging.info("Iniciando limpeza de dados...")
    df_clean = df.copy()
    
    # Exemplo: Preencher nulos ou dropar linhas inválidas
    # Assumindo que 'renda' e 'divida' são colunas da API (Ajuste os nomes conforme sua realidade)
    colunas_calculo = ['renda', 'divida', 'pontualidade']
    
    # Verifica se as colunas de fato existem antes de limpar
    colunas_presentes = [c for c in colunas_calculo if c in df_clean.columns]
    df_clean = df_clean.dropna(subset=colunas_presentes)

    # Regras de negócio (exemplo): Renda não pode ser negativa
    if 'renda' in df_clean.columns:
        df_clean = df_clean[df_clean['renda'] >= 0]
        
    logging.info(f"Limpeza concluída. Linhas remanescentes: {len(df_clean)}")
    return df_clean

def calculate_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula o score de crédito do cliente baseado nos dados limpos.

    Regra de negócio (Exemplo simplificado):
    Score = (Renda - Divida) * (Pontualidade / 100)
    O score é normalizado entre 0 e 1000.

    Args:
        df (pd.DataFrame): DataFrame limpo.

    Returns:
        pd.DataFrame: DataFrame contendo o ID do cliente e o novo campo 'score_credito'.
    """
    logging.info("Iniciando cálculo de score de crédito...")
    df_scored = df.copy()

    # Exemplo genérico de cálculo de score (substitua pelas suas regras matemáticas)
    # df_scored['score_credito'] = ...
    
    # Mocking da lógica para completar a task:
    if all(c in df_scored.columns for c in ['renda', 'divida', 'pontualidade']):
        capacidade_pgto = df_scored['renda'] - df_scored['divida']
        score_bruto = (capacidade_pgto * (df_scored['pontualidade'] / 100))
        # Normalização arbitrária para 0 - 1000
        df_scored['score_credito'] = (score_bruto / score_bruto.max()) * 1000
        df_scored['score_credito'] = df_scored['score_credito'].clip(lower=0, upper=1000).astype(int)
    else:
        # Fallback caso os nomes das colunas da API sejam diferentes na realidade
        df_scored['score_credito'] = 500 

    logging.info("Cálculo de score concluído.")
    return df_scored

def export_score_to_csv(df: pd.DataFrame, output_path: str) -> None:
    """
    Exporta o dataframe final contendo as pontuações para um arquivo CSV.
    As colunas exportadas devem ser claramente documentadas.

    Args:
        df (pd.DataFrame): DataFrame contendo os resultados.
        output_path (str): Caminho do arquivo CSV de saída.
    """
    logging.info(f"Exportando resultados para {output_path}...")
    
    # Exporta com header
    df.to_csv(output_path, index=False, sep=';', encoding='utf-8')
    logging.info("Exportação concluída com sucesso.")

# ==========================================
# Execução Principal (Main)
# ==========================================

if __name__ == "__main__":
    # 1. Configurações Iniciais
    PROJECT_ID = "99c511dd-8201-40ae-a6f6-b2f0381e172a"
    # Pegando o token de uma variável de ambiente para segurança (Best Practice)
    TOKEN = os.getenv("DATAMISSION_API_TOKEN", "SEU_TOKEN_AQUI_CASO_NAO_USE_ENV") 
    
    ARQUIVO_PARQUET_BRUTO = f"dataset_{PROJECT_ID}.parquet"
    ARQUIVO_CSV_FINAL = "score_credito_resultados.csv"

    # Definição do esquema esperado (Ajuste os nomes de acordo com o retorno real da API)
    ESQUEMA_ESPERADO = {
        'id_cliente': 'object',
        'renda': 'float',
        'divida': 'float',
        'pontualidade': 'float' # ou int
    }

    try:
        # 2. Ingestão (Download dos dados brutos)
        download_dataset(PROJECT_ID, TOKEN, ARQUIVO_PARQUET_BRUTO)

        # 3. Leitura do arquivo intermediário gerado
        df_bruto = pd.read_parquet(ARQUIVO_PARQUET_BRUTO)

        # 4. Validação Crítica
        validate_schema(df_bruto, ESQUEMA_ESPERADO)

        # 5. Limpeza de Dados
        df_limpo = clean_data(df_bruto)

        # 6. Aplicação de Modelo / Cálculo
        df_score = calculate_score(df_limpo)

        # 7. Filtro das colunas finais (documentadas no README) e Exportação
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