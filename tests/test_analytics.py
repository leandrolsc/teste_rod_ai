import pytest
import pandas as pd
from src.analytics import calcular_cross_sell, calcular_sequencia_visualizacao

@pytest.fixture
def logs_mock():
    """
    Cria um DataFrame mock imitando logs de navegação e compra.
    """
    data = [
        # Sessão 1: Visualiza A, Visualiza B, Compra A, Compra B
        {"session_id": "s1", "item_id": "item_A", "action": "view", "timestamp": "2026-03-20 10:00:00"},
        {"session_id": "s1", "item_id": "item_B", "action": "view", "timestamp": "2026-03-20 10:05:00"},
        {"session_id": "s1", "item_id": "item_A", "action": "purchase", "timestamp": "2026-03-20 10:10:00"},
        {"session_id": "s1", "item_id": "item_B", "action": "purchase", "timestamp": "2026-03-20 10:11:00"},
        
        # Sessão 2: Visualiza A, Visualiza C, Compra A, Compra C
        {"session_id": "s2", "item_id": "item_A", "action": "view", "timestamp": "2026-03-20 11:00:00"},
        {"session_id": "s2", "item_id": "item_C", "action": "view", "timestamp": "2026-03-20 11:05:00"},
        {"session_id": "s2", "item_id": "item_A", "action": "purchase", "timestamp": "2026-03-20 11:10:00"},
        {"session_id": "s2", "item_id": "item_C", "action": "purchase", "timestamp": "2026-03-20 11:11:00"},
        
        # Sessão 3: Visualiza A, Visualiza B, Compra B
        {"session_id": "s3", "item_id": "item_A", "action": "view", "timestamp": "2026-03-20 12:00:00"},
        {"session_id": "s3", "item_id": "item_B", "action": "view", "timestamp": "2026-03-20 12:05:00"},
        {"session_id": "s3", "item_id": "item_B", "action": "purchase", "timestamp": "2026-03-20 12:10:00"},
    ]
    return pd.DataFrame(data)

def test_calcular_cross_sell(logs_mock):
    df_cross_sell = calcular_cross_sell(logs_mock)
    
    # Esperamos encontrar as combinações (item_A, item_B) e (item_A, item_C)
    assert len(df_cross_sell) == 2
    
    # Verifica a frequência da combinação A e B (aconteceu na sessão s1)
    ab_freq = df_cross_sell[
        (df_cross_sell['item_id_x'] == 'item_A') & 
        (df_cross_sell['item_id_y'] == 'item_B')
    ]['frequencia_compra'].iloc[0]
    
    assert ab_freq == 1

def test_calcular_sequencia_visualizacao(logs_mock):
    df_seq = calcular_sequencia_visualizacao(logs_mock)
    
    # O item A é seguido pelo item B 2 vezes (s1, s3)
    ab_seq_freq = df_seq[
        (df_seq['item_id'] == 'item_A') & 
        (df_seq['next_item_id'] == 'item_B')
    ]['frequencia_visualizacao'].iloc[0]
    
    assert ab_seq_freq == 2
    
    # O item A é seguido pelo item C 1 vez (s2)
    ac_seq_freq = df_seq[
        (df_seq['item_id'] == 'item_A') & 
        (df_seq['next_item_id'] == 'item_C')
    ]['frequencia_visualizacao'].iloc[0]
    
    assert ac_seq_freq == 1
