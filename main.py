import time
import schedule
import logging
import sys

from config import PROJECT_ID, API_BASE_URL, OUTPUT_DIR, POLL_INTERVAL_MINUTES
from api_client import fetch_traffic_logs
from processor import process_and_save

# Configuração global de Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("TrafficPipeline")

def job():
    """
    Função principal que coordena a extração e processamento.
    """
    logger.info("="*50)
    logger.info("Iniciando rotina de ingestão de logs de tráfego...")
    
    try:
        # 1. Extração
        raw_data = fetch_traffic_logs(API_BASE_URL, PROJECT_ID)
        
        # 2. Transformação e Carga (Save)
        process_and_save(raw_data, OUTPUT_DIR)
        
        logger.info("Rotina finalizada com sucesso.")
    except Exception as e:
        logger.error(f"Falha na execução da rotina de ingestão: {e}")
    finally:
        logger.info("="*50)

if __name__ == "__main__":
    logger.info(f"Serviço inicializado. As coletas ocorrerão a cada {POLL_INTERVAL_MINUTES} minuto(s).")
    
    # Executa a primeira vez imediatamente ao subir o container
    job()
    
    # Agenda as próximas execuções de acordo com a configuração
    schedule.every(POLL_INTERVAL_MINUTES).minutes.do(job)
    
    # Loop infinito para manter o agendador rodando
    while True:
        schedule.run_pending()
        time.sleep(1)