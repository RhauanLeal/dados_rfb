#!/bin/sh
set -e

echo "Iniciando ETL RFB..."
python etl_rfb2.py "$@"
echo "ETL finalizado."
