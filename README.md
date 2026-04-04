# Pipeline de Ingestão e Cálculo de Score de Crédito

Este repositório contém a rotina automatizada para baixar dados brutos da API da Datamission, processá-los com segurança e gerar uma pontuação (score) de crédito em formato CSV.

## 📋 Pré-requisitos

Certifique-se de ter o Python 3.8+ instalado. Você precisará das seguintes bibliotecas:

```bash
pip install requests pandas pyarrow 