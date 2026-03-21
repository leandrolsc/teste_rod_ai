import pytest
import pandas as pd
import json
import os
from src.transformations import realizar_parse_json, validar_e_limpar_dados

# Fixture para criar um arquivo JSON temporário para testes
@pytest.fixture
def json_temporario(tmpdir):
    dados_mock = [
        {"id": 1, "Nome": "Projeto Alfa", "Status": "Ativo"},
        {"id": None, "Nome": "Projeto Invalido", "Status": "Pendente"}, # ID nulo, deve ser removido
        {"id": 3, "Nome": "Projeto Beta", "Status": None} # Status nulo, deve virar "N/A"
    ]
    arquivo = tmpdir.join("dados_teste.json")
    with open(arquivo, 'w') as f:
        json.dump(dados_mock, f)
    return str(arquivo)

def test_realizar_parse_json(json_temporario):
    """
    Testa se o parser lê o JSON e converte para DataFrame corretamente.
    """
    df = realizar_parse_json(json_temporario)
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert list(df.columns) == ["id", "Nome", "Status"]

def test_validar_e_limpar_dados():
    """
    Testa as regras de limpeza: 
    - Remoção de nulos em 'id'
    - Lowercase em colunas
    - Preenchimento de texto nulo com 'N/A'
    """
    df_cru = pd.DataFrame([
        {"id": 1, "Nome": "A", "Status": "Ativo"},
        {"id": pd.NA, "Nome": "B", "Status": "Pendente"},
        {"id": 3, "Nome": "C", "Status": None}
    ])
    
    df_limpo = validar_e_limpar_dados(df_cru)
    
    # Verifica se a linha com id nulo foi removida (espera 2 linhas restantes)
    assert len(df_limpo) == 2
    
    # Verifica se as colunas ficaram em letras minúsculas
    assert list(df_limpo.columns) == ["id", "nome", "status"]
    
    # Verifica o preenchimento de nulos ("N/A")
    registro_id_3 = df_limpo[df_limpo['id'] == 3].iloc[0]
    assert registro_id_3['status'] == "N/A"
