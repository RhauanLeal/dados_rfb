#!/bin/sh
set -e

echo "Iniciando ETL RFB..."
python etl_rfb_dados.py
echo "ETL finalizado."
