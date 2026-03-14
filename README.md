Dataset Processor (v2.0)

Este projeto realiza o download, persistência temporária e processamento de datasets via API.

Formatos Suportados

O sistema suporta os seguintes formatos via parâmetro de consulta:

Parquet (?format=parquet): Formato padrão, recomendado para grandes volumes.

JSON (?format=json): Utilizado para transformações rápidas e debug.

CSV, LOG, XML: Suportados para exportações legadas.

Lógica de Seleção de Formato

O script decide o formato com base na seguinte prioridade:

Argumento Explícito: Caso o usuário passe file_format na chamada da função.

Validação: Se o formato solicitado não estiver na lista de suportados (parquet, json, csv, log, xml), o sistema faz o fallback automático para o formato padrão configurado no cliente (Default: parquet).

Fluxo de Execução

Download: A API é chamada com o query param adequado.

Persistência: O conteúdo é gravado em um arquivo temporário (/tmp ou similar) antes de qualquer leitura. Isso garante que falhas de memória não ocorram com datasets gigantes.

Validação: O código verifica o status HTTP antes de liberar o arquivo para o processador.

Uso

client = DatasetClient(base_url="...", api_token="...", default_format="parquet")

# Baixa em parquet por padrão e salva em arquivo temp
caminho_arquivo = client.get_dataset(project_id="123") 

# Força download em JSON
caminho_json = client.get_dataset(project_id="123", file_format="json")


Testes

Os testes validam a construção da URL, a escrita física do arquivo no disco e a lógica de fallback de formatos:

python test_project.py
