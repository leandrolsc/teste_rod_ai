import pandas as pd
from datetime import datetime

def transform_claims_data(df: pd.DataFrame):
    """
    Normaliza timestamps e tipos de sinistro antes da agregação.
    """
    # 1. Normalização de Timestamps
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df = df.dropna(subset=['timestamp']) # Remove registros com data inválida
    
    # 2. Normalização de Tipos de Sinistro (Ex: 'roubo' -> 'ROUBO')
    if 'tipo_sinistro' in df.columns:
        df['tipo_sinistro'] = df['tipo_sinistro'].str.strip().str.upper()

    # 3. Agregação Temporal (Mesma lógica do SQL de teste)
    df['data_referencia'] = df['timestamp'].dt.date
    aggregated = df.groupby(['data_referencia', 'tipo_sinistro']).agg({
        'valor': 'sum'
    }).reset_index()

    # 4. Rastreabilidade e Cabeçalho
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    aggregated['meta_source_api'] = f"DataMission_v1"
    aggregated['meta_processed_at'] = now_str
    
    return aggregated