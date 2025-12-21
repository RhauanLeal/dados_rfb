# Dockerfile
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Instala dependências do sistema, incluindo locales e curl para healthcheck
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    postgresql-client \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Usa locale padrão C.UTF-8 (já incluída na imagem)
ENV LANG=C.UTF-8 \
    LC_ALL=C.UTF-8

WORKDIR /code

# Copia e instala dependências primeiro (cache de layers)
COPY requirements.txt .

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copia o projeto
COPY . .

# Cria diretórios necessários
RUN mkdir -p logs

# Aqui esse arquivo ja executa o ETL
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
