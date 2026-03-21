import pandas as pd
import logging
from typing import Tuple

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def calcular_cross_sell(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula indicadores de cross-sell (itens comprados juntos na mesma sessão).
    Espera-se que o DataFrame contenha as colunas: 'session_id', 'item_id', 'action'
    onde action == 'purchase' representa uma compra.
    """
    logging.info("Calculando indicadores de cross-sell...")
    
    # Filtra apenas eventos de compra
    df_compras = df[df['action'] == 'purchase'][['session_id', 'item_id']]
    
    # Faz um self-join para encontrar pares de itens na mesma sessão
    df_pares = df_compras.merge(df_compras, on='session_id')
    
    # Remove pares onde o item é o mesmo e remove duplicatas espelhadas (A,B e B,A)
    df_pares = df_pares[df_pares['item_id_x'] < df_pares['item_id_y']]
    
    # Conta a frequência dos pares
    cross_sell_df = df_pares.groupby(['item_id_x', 'item_id_y']).size().reset_index(name='frequencia_compra')
    cross_sell_df = cross_sell_df.sort_values(by='frequencia_compra', ascending=False).reset_index(drop=True)
    
    logging.info(f"Cálculo concluído. Encontrados {len(cross_sell_df)} pares de cross-sell.")
    return cross_sell_df

def calcular_sequencia_visualizacao(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula a sequência de visualização (qual item é visto logo após o outro).
    Espera-se que o DataFrame contenha as colunas: 'session_id', 'item_id', 'action', 'timestamp'
    onde action == 'view' representa uma visualização.
    """
    logging.info("Calculando sequência de visualização...")
    
    # Filtra apenas eventos de visualização e ordena cronologicamente por sessão
    df_views = df[df['action'] == 'view'].sort_values(['session_id', 'timestamp'])
    
    # Cria uma coluna com o próximo item visualizado na mesma sessão
    df_views['next_item_id'] = df_views.groupby('session_id')['item_id'].shift(-1)
    
    # Remove as últimas visualizações das sessões (onde não há "próximo item")
    df_sequencia = df_views.dropna(subset=['next_item_id'])
    
    # Conta a frequência das sequências
    seq_df = df_sequencia.groupby(['item_id', 'next_item_id']).size().reset_index(name='frequencia_visualizacao')
    seq_df = seq_df.sort_values(by='frequencia_visualizacao', ascending=False).reset_index(drop=True)
    
    logging.info(f"Cálculo concluído. Encontradas {len(seq_df)} sequências de visualização.")
    return seq_df

def salvar_indicadores_csv(df: pd.DataFrame, caminho_destino: str) -> None:
    """
    Salva os indicadores gerados em formato CSV (suportado e legível).
    """
    logging.info(f"Salvando indicadores analíticos em: {caminho_destino}")
    try:
        df.to_csv(caminho_destino, index=False)
        logging.info("Indicadores salvos com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao salvar indicadores: {e}")
        raise
