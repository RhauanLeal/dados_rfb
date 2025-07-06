#!/bin/bash
set -euo pipefail

# Diretório do projeto
cd "$(dirname "$0")"

# Verifica se o Python do venv existe
if [ ! -x "venv/bin/python" ]; then
    echo "❌ Ambiente virtual não encontrado ou inválido. Verifique se a pasta 'venv/' existe."
    exit 1
fi

# Garante que o diretório de logs existe
LOG_DIR="logs"
mkdir -p "$LOG_DIR"

# Verifica permissões de escrita no diretório de logs
if [ ! -w "$LOG_DIR" ]; then
    echo "❌ Sem permissão de escrita no diretório de logs: $LOG_DIR"
    echo "   Use: sudo chown -R $(whoami):$(whoami) $LOG_DIR"
    exit 1
fi

# Timestamp para o nome do log
DATA_HORA=$(date "+%Y-%m-%d_%H-%M-%S")
LOG_ETL="$LOG_DIR/etl_$DATA_HORA.log"

echo "🟡 Iniciando ETL em segundo plano às $(date)"
nohup ./venv/bin/python etl_rfb_dados.py --etl >> "$LOG_ETL" 2>&1 &
ETL_PID=$!
echo "🔄 Processo ETL rodando com PID $ETL_PID"
echo "ETL iniciado em segundo plano."

