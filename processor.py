import pandas as pd
import os
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def validate_data(df: pd.DataFrame) -> bool:
    """
    Valida se o DataFrame contém as colunas mínimas esperadas.
    """
    required_columns = ['timestamp', 'event_type', 'user_id']
    missing = [col for col in required_columns if col not in df.columns]
    
    if missing:
        raise ValueError(f"Falha na validação: Colunas obrigatórias ausentes -> {missing}")
    
    logger.info("Validação de dados concluída. Estrutura validada com sucesso.")
    return True

def process_and_save(raw_data: list, output_dir: str):
    """
    Recebe os dados brutos, converte para pandas, valida, agrega e salva em CSV e JSON.
    """
    if not raw_data:
        logger.warning("Nenhum dado recebido da API para processar.")
        return

    try:
        # 1. Carga dos dados para o Pandas DataFrame
        df = pd.DataFrame(raw_data)

        # 2. Validação dos dados
        validate_data(df)

        # 3. Transformação
        # Convertendo string de data/hora para o tipo datetime do Pandas
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['data_referencia'] = df['timestamp'].dt.date

        # 4. Agregação
        # Contagem de eventos (tráfego e conversão) por dia e por tipo de evento
        agg_df = df.groupby(['data_referencia', 'event_type']).size().reset_index(name='quantidade_eventos')
        
        # Conta usuários únicos por dia
        users_df = df.groupby(['data_referencia'])['user_id'].nunique().reset_index(name='usuarios_unicos')
        
        # Mesclando as agregações
        final_agg_df = pd.merge(agg_df, users_df, on='data_referencia', how='left')

        # 5. Salvar Arquivos
        os.makedirs(output_dir, exist_ok=True)
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")

        raw_filepath = os.path.join(output_dir, f"raw_logs_{timestamp_str}.json")
        processed_filepath = os.path.join(output_dir, f"aggregated_traffic_{timestamp_str}.csv")

        # Salva o log bruto completo para auditoria/datalake (JSON)
        df.to_json(raw_filepath, orient='records', indent=4)
        
        # Salva a tabela agregada para o Power BI (CSV)
        final_agg_df.to_csv(processed_filepath, index=False, sep=';', encoding='utf-8')

        logger.info(f"Arquivos gerados com sucesso:")
        logger.info(f" - Bruto: {raw_filepath}")
        logger.info(f" - Processado: {processed_filepath}")

    except Exception as e:
        logger.error(f"Erro inesperado durante o processamento e transformação dos dados: {e}")
        raise