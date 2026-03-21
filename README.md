Pipeline de Ingestão de Dados e Modelagem Analítica

Este projeto contém uma pipeline de dados automatizada (DAG) construída com Apache Airflow. A pipeline é responsável por extrair dados de uma API, processá-los utilizando Pandas, derivar métricas de negócio (cross-sell e sequência de visualização) e salvá-los em formatos otimizados.

📌 Responsabilidade de Cada Etapa

Extração (Ingestão): A DAG faz uma requisição GET para o endpoint /projects/{project_id}/dataset e salva o payload bruto (raw) no formato JSON.

Transformação: O módulo src/transformations.py contém funções puras em Pandas. Ele lê o JSON bruto, realiza a limpeza dos dados (remoção de nulos em campos chaves, padronização de strings) e os prepara.

Carga (Armazenamento): Os dados limpos são salvos no formato Parquet (suportado e altamente otimizado para analytics).

Modelagem Analítica: O módulo src/analytics.py processa os logs de navegação gerando:

Cross-sell: Frequência de itens comprados juntos na mesma sessão.

Sequência de Visualização: Frequência de qual item é visto imediatamente após o outro.

Estes resultados são salvos em CSV.

Validação (Testes): O diretório tests/ garante a integridade do parse do JSON, das limpezas e da matemática por trás das métricas analíticas.

🚀 Passo a Passo: Como Reproduzir Localmente

Siga estas instruções para executar o pipeline em sua máquina e observar os resultados gerados.

1. Preparando o Ambiente

Recomenda-se utilizar um ambiente virtual Python (versão 3.8+).

# Criar e ativar o ambiente virtual
python -m venv venv
source venv/bin/activate  # No Windows: venv\Scripts\activate

# Instalar as dependências
pip install -r requirements.txt


2. Executando os Testes de Validação

Antes de rodar a pipeline, garanta que todas as funções de parse e modelagem estão corretas rodando a suíte de testes automatizados:

pytest tests/ -v


Você verá a confirmação de que os testes de parse do arquivo da API, limpeza e cálculo de métricas passaram com sucesso.

3. Executando a Pipeline (Airflow)

Para testar a DAG localmente sem precisar iniciar a interface web pesada do Airflow, usaremos o CLI:

# Define o diretório atual como a pasta principal do Airflow
export AIRFLOW_HOME=$(pwd)

# Inicializa o banco de metadados SQLite localmente
airflow db init

# Passo 1: Ingestão (Baixa o JSON da API)
airflow tasks test ingestao_dados_projeto extrair_dados_api 2026-03-20

# Passo 2: Transformação (Limpa e salva em Parquet)
airflow tasks test ingestao_dados_projeto transformar_e_salvar_dados 2026-03-20

# Passo 3: Modelagem (Gera CSVs de Cross-sell e Sequência)
airflow tasks test ingestao_dados_projeto modelagem_analitica 2026-03-20


4. 📂 Observando os Outputs

Após a execução com sucesso dos comandos acima, os arquivos estarão disponíveis no diretório temporário configurado (/tmp/data_pipeline por padrão no Linux/Mac, ou o equivalente no Windows).

Verifique os arquivos gerados:

raw_dataset_projeto_xyz_123.json: O payload original baixado da API (formato JSON).

processed_dataset_projeto_xyz_123.parquet: Os dados limpos e tipados, prontos para consumo por ferramentas de BI (formato Parquet).

cross_sell_projeto_xyz_123.csv: Indicadores de itens comprados juntos (formato CSV).

views_sequence_projeto_xyz_123.csv: Indicadores da jornada de visualização do usuário (formato CSV).

(Dica: Você pode abrir os arquivos .csv e .json em qualquer editor de texto ou no Excel. Para ler o .parquet, você pode usar um script rápido em Python com pd.read_parquet()).
