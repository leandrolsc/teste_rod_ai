# Usar imagem oficial e leve do Python
FROM python:3.10-slim

# Definir diretório de trabalho dentro do contêiner
WORKDIR /app

# Atualizar pacotes do sistema e limpar cache
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Copiar arquivo de dependências
COPY requirements.txt .

# Instalar dependências Python
RUN pip install --no-cache-dir -r requirements.txt

# Copiar todo o código fonte para o contêiner
COPY . .

# Criar o diretório de output dos dados
RUN mkdir -p /app/data

# Definir variáveis de ambiente padrão (podem ser sobrescritas no docker run)
ENV PYTHONUNBUFFERED=1
ENV OUTPUT_DIR=/app/data
ENV POLL_INTERVAL_MINUTES=60

# Comando para iniciar o pipeline
ENTRYPOINT ["python", "main.py"]