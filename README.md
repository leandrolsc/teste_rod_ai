Dataset Downloader & Processor

Este repositório contém um script robusto para baixar datasets de projetos via API REST e realizar transformações de dados para análise posterior.

Requisitos

Python 3.8+

Biblioteca requests

Instale as dependências:

pip install requests


Estrutura do Projeto

api_client.py: Encapsula as chamadas HTTP e tratamento de erros.

transformations.py: Funções puras para processamento e limpeza dos dados.

test_project.py: Testes automatizados para garantir a qualidade do código.

Como Executar

O script principal (ou integração) requer dois parâmetros fundamentais para autenticação e localização da API.

Parâmetros da API

Parâmetro

Descrição

Exemplo

BASE_URL

URL base do servidor API

https://api.empresa.com

API_TOKEN

Token de autenticação Bearer

e9f8...a12

PROJECT_ID

Identificador único do projeto

654321

Exemplo de Uso Rápido

from api_client import DatasetClient
from transformations import clean_data, aggregate_by_category

client = DatasetClient("[https://api.suaempresa.com](https://api.suaempresa.com)", "seu_token")
raw_data = client.get_dataset("ID_DO_PROJETO")

if raw_data:
    data = clean_data(raw_data)
    summary = aggregate_by_category(data)
    print("Resumo por Categoria:", summary)


Executando Testes

Para validar o comportamento do sistema sem consumir créditos da API real, execute:

python test_project.py
