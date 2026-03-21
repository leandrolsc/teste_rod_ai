import pandas as pd
import json
import logging
from typing import Dict, Any

# Configuração básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def realizar_parse_json(caminho_arquivo_json: str) -> pd.DataFrame:
    """
    Lê o arquivo JSON salvo localmente e o converte para um DataFrame Pandas.
    """
    logging.info(f"Iniciando leitura do arquivo JSON em: {caminho_arquivo_json}")
    try:
        df = pd.read_json(caminho_arquivo_json)
        logging.info(f"Parse concluído. DataFrame com {len(df)} linhas carregado.")
        return df
    except Exception as e:
        logging.error(f"Erro ao realizar o parse do JSON: {e}")
        raise

def validar_e_limpar_dados(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica validações e limpezas básicas nos dados transformando-os para uso subsequente.
    Mantido separado para permitir testes unitários isolados.
    """
    logging.info("Iniciando limpeza e validação de dados.")
    
    # 1. Remover linhas onde colunas críticas estão vazias (exemplo genérico)
    # Assumimos que 'id' é uma coluna obrigatória se existir no payload
    if 'id' in df.columns:
        df = df.dropna(subset=['id'])
    
    # 2. Padronizar nomes de colunas para minúsculo
    df.columns = [col.lower() for col in df.columns]
    
    # 3. Preencher valores nulos em colunas de texto
    # (Adaptar conforme a regra de negócio do seu dataset)
    df = df.fillna("N/A")
    
    logging.info(f"Limpeza concluída. Restaram {len(df)} registros válidos.")
    return df

def salvar_como_parquet(df: pd.DataFrame, caminho_destino: str) -> None:
    """
    Salva o DataFrame transformado em formato Parquet.
    """
    logging.info(f"Salvando dados transformados no formato Parquet em: {caminho_destino}")
    try:
        # pyarrow é o engine padrão recomendado para parquet no pandas
        df.to_parquet(caminho_destino, engine='pyarrow', index=False)
        logging.info("Arquivo Parquet salvo com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao salvar arquivo Parquet: {e}")
        raise
