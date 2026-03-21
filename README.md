Pipeline de Ingestão de Dados do Projeto

Este projeto contém uma pipeline de dados automatizada (DAG) construída com Apache Airflow. A pipeline é responsável por extrair dados de uma API, processá-los utilizando Pandas e salvá-los em formatos otimizados localmente/temporariamente.

Responsabilidade de Cada Etapa

Extração (Ingestão): A DAG faz uma requisição GET para o endpoint /projects/{project_id}/dataset e salva o payload bruto (raw) no formato JSON em um diretório temporário.

Transformação: O arquivo src/transformations.py contém funções puras e reutilizáveis em Pandas. Ele lê o JSON bruto, realiza a limpeza dos dados (remoção de nulos, padronização) e os prepara.

Carga (Armazenamento): Após a transformação, os dados são salvos no formato Parquet (suportado e altamente otimizado para analytics).

Testes Automatizados: O diretório tests/ contém testes unitários usando pytest para garantir que o parse e a validação/limpeza dos dados funcionem conforme o esperado.

Como Executar Localmente

1. Pré-requisitos

Certifique-se de ter o Python 3.8+ instalado. Recomenda-se o uso de um ambiente virtual (venv).

2. Instalação das Dependências

Instale as bibliotecas necessárias executando:

pip install -r requirements.txt


3. Executando os Testes Unitários

Para rodar a validação e o parse básico dos dados, utilize o Pytest:

pytest tests/


4. Executando a Pipeline (Airflow Local)

Como é um projeto Airflow, você pode testar as tarefas individualmente sem precisar subir todo o webserver do Airflow, usando o comando airflow tasks test:

# Definir a pasta de dags local
export AIRFLOW_HOME=$(pwd)

# Inicializar o banco de dados do Airflow (apenas na primeira vez)
airflow db init

# Testar a extração
airflow tasks test ingestao_dados_projeto extrair_dados_api 2026-01-01

# Testar a transformação e salvamento
airflow tasks test ingestao_dados_projeto transformar_e_salvar_dados 2026-01-01


(Nota: Certifique-se de configurar a variável de ambiente API_BASE_URL ou usar um mock local da API caso a URL de produção exija autenticação real).
