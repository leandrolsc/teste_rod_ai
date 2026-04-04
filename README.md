Pipeline de Ingestão e Score de Risco/Fraude
Este repositório contém a rotina automatizada para extrair dados brutos (transacionais e de crédito) da API da Datamission, aplicar regras de higienização, gerar pontuações de anomalias/crédito e exportar os resultados com total rastreabilidade de lote.

📋 Pré-requisitos
Certifique-se de ter o Python 3.8+ instalado. Você precisará das seguintes bibliotecas:

Bash
pip install requests pandas pyarrow
🔄 Como Reexecutar o Pipeline para Novos Lotes
A rotina foi desenhada para processar lotes contínuos de dados. Para reexecutar o pipeline e processar as informações mais recentes da API, siga os passos abaixo:

1. Obtenha as Credenciais Atualizadas
Certifique-se de ter um Token JWT válido gerado pela plataforma da Datamission para autorizar o download do novo dataset.

2. Defina as Variáveis de Ambiente
Não insira o token diretamente no código. Configure-o no seu terminal ou ambiente de execução (ex: GitHub Actions, Airflow):

Linux / macOS:

Bash
export DATAMISSION_API_TOKEN="seu_novo_token_aqui"
Windows (PowerShell):

PowerShell
$env:DATAMISSION_API_TOKEN="seu_novo_token_aqui"
3. Execute os Scripts
Dispare a execução principal. O pipeline fará o download do novo lote em formato Parquet, realizará as validações de esquema e aplicará as regras de negócio:

Bash
python scoring_pipeline.py
4. Valide a Saída
Verifique o diretório local. Um novo arquivo CSV (ex: score_resultados.csv) será gerado. Confirme se a coluna versao_lote_api foi atualizada com o metadado da execução atual, garantindo que o lote foi processado com sucesso.

🔗 Integração com Alertas de Fraude
Os CSVs gerados por este pipeline não são o fim do processo, mas sim o gatilho para a tomada de decisão. Para integrar os resultados de risco com a arquitetura de segurança da empresa, siga este fluxo de consumo:

1. Monitoramento da Saída (File Watching):
Configure um agente (como Filebeat, Logstash ou um watchdog em Python) para monitorar o diretório de destino do pipeline. Assim que o script finaliza a execução e salva o novo CSV, o agente detecta a modificação e inicia a leitura das novas linhas.

2. Publicação em Tópicos de Mensageria:
O agente deve capturar os registros extraídos do CSV e publicá-los como eventos em um tópico de mensageria (ex: Apache Kafka, AWS SNS/SQS ou RabbitMQ). Isso desacopla o processamento e permite que múltiplos sistemas consumam os resultados simultaneamente.

3. Alimentação de Dashboards de Risco:
Os eventos trafegados na mensageria devem ser ingeridos por ferramentas de SIEM ou visualização de dados (ex: Splunk, ElasticSearch/Kibana, Datadog). Isso consolida o histórico de anomalias, permitindo que os analistas visualizem a evolução do risco dos clientes.

4. Gatilhos de Ação (Thresholds):
Consumidores conectados à mensageria devem aplicar regras automatizadas. Por exemplo: se um evento chega com score_anomalia >= 85, o microsserviço antifraude consome a mensagem e dispara imediatamente uma chamada à API do Core Banking para bloqueio preventivo do cliente, enviando paralelamente um alerta para o canal do Slack da equipe de auditoria.