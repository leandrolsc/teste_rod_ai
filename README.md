Pipeline de Consumo de Eventos de Tráfego

Este projeto consiste em um pipeline de dados em Python que consome logs de tráfego e conversão de uma API, aplica validações, realiza agregações utilizando pandas e exporta os dados estruturados para análise em ferramentas de BI (como Power BI).

🏗️ Arquitetura do Pipeline

Ingestão (Extração): O módulo api_client.py faz chamadas regulares (GET) ao endpoint /projects/{project_id}/dataset?format=log.

Validação: Verifica se os dados crus possuem as colunas obrigatórias (timestamp, event_type, user_id).

Tratamento (Transformação): O módulo processor.py converte datas, agrupa os eventos diários por tipo e conta a quantidade de usuários únicos.

Carga (Carregamento): Salva um backup bruto em .json e um arquivo estruturado e consolidado em .csv.

🚀 Como Executar com Docker

Para rodar o projeto em um contêiner isolado, siga os passos:

Construir a imagem:

docker build -t traffic-pipeline .


Rodar o contêiner (mapeando o volume para acessar os dados gerados):

docker run -d \
  --name traffic-job \
  -e PROJECT_ID="seu_projeto_id" \
  -e API_BASE_URL="[https://sua-api.com](https://sua-api.com)" \
  -e POLL_INTERVAL_MINUTES="30" \
  -v $(pwd)/meus_dados:/app/data \
  traffic-pipeline


Nota: O parâmetro -v $(pwd)/meus_dados:/app/data garante que os arquivos CSV e JSON gerados dentro do Docker sejam salvos na pasta meus_dados da sua máquina física.

📊 Como Carregar os Arquivos no Power BI

Como a rotina gera arquivos regularmente (com carimbo de data/hora), a melhor forma de consumir no Power BI é conectando-se à Pasta onde os arquivos estão sendo salvos:

Abra o Power BI Desktop.

Clique em Obter Dados -> Mais...

Selecione a opção Pasta (ou Folder) e clique em Conectar.

Insira o caminho da pasta onde o Docker está salvando os arquivos (ex: C:\caminho\para\meus_dados).

Clique em Transformar Dados (não clique em Combinar ainda).

Na coluna Name, filtre apenas os arquivos que começam com aggregated_traffic_ (ou termine em .csv).

Na coluna Content, clique no ícone de duas setas para baixo (🔽) para Combinar Arquivos.

O Power BI irá empilhar todos os CSVs em uma única tabela contínua.

Feche e aplique. Agora seus dashboards de Tráfego e Conversão serão atualizados automaticamente à medida que novos arquivos chegam à pasta!