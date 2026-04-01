import pandas as pd

def aggregate_temporal_data(df: pd.DataFrame):
    """
    Realiza a agregação temporal dos dados para consolidar métricas por dia.
    
    Tabela Final Esperada:
    | Coluna       | Tipo     | Descrição                          |
    |--------------|----------|------------------------------------|
    | data         | datetime | Data da transação                  |
    | total_valor  | float    | Soma dos valores no período        |
    | source_info  | string   | Origem do dado (API + Timestamp)   |
    """
    # Exemplo de lógica de agregação (Mesmo SQL usado nos testes)
    df['data'] = pd.to_datetime(df['timestamp']).dt.date
    aggregated = df.groupby('data')['valor'].sum().reset_index()
    
    # Adicionando referenciamento à fonte (Rastreabilidade)
    now = datetime.datetime.now().isoformat()
    aggregated['source_info'] = f"API_DataMission_v1_at_{now}"
    
    return aggregated