import pytest
import pandas as pd
import json
from src.transformations import realizar_parse_json, validar_e_limpar_dados

@pytest.fixture
def json_payload_api(tmpdir):
    """
    Simula o payload exato retornado pelo endpoint GET /projects/{project_id}/dataset
    incluindo casos que precisam de validação (nulos, letras maiúsculas).
    """
    dados_api = [
        {"id": "evt_001", "session_id": "s1", "item_id": "item_A", "action": "view", "timestamp": "2026-03-20T10:00:00Z"},
        {"id": None, "session_id": "s1", "item_id": "item_B", "action": "VIEW", "timestamp": "2026-03-20T10:05:00Z"}, # ID Nulo
        {"id": "evt_003", "session_id": "s2", "item_id": "item_A", "action": "purchase", "timestamp": None} # Timestamp nulo
    ]
    arquivo = tmpdir.join("api_response_mock.json")
    with open(arquivo, 'w') as f:
        json.dump(dados_api, f)
    return str(arquivo)

def test_validar_parse_arquivo_baixado(json_payload_api):
    """
    Assegura o parse correto do arquivo baixado da API para as estruturas do Pandas.
    """
    df = realizar_parse_json(json_payload_api)
    
    # Verifica se os tipos básicos foram respeitados no parse
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    # Verifica se as colunas vieram corretamente do payload
    assert "session_id" in df.columns
    assert "action" in df.columns

def test_validacao_limpeza_dados_api(json_payload_api):
    """
    Assegura que as validações básicas (remoção de nulos essenciais, 
    padronização) estão ocorrendo na saída bruta da API.
    """
    df_raw = realizar_parse_json(json_payload_api)
    df_clean = validar_e_limpar_dados(df_raw)
    
    # Linha com 'id' nulo (evt_002) deve ter sido descartada
    assert len(df_clean) == 2
    
    # Nomes de colunas devem estar em lowercase
    assert all(col.islower() for col in df_clean.columns)
    
    # Valores nulos não críticos devem ter sido preenchidos com N/A
    registro_sem_timestamp = df_clean[df_clean['id'] == 'evt_003'].iloc[0]
    assert registro_sem_timestamp['timestamp'] == "N/A"
