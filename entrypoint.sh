#!/bin/sh
set -e

echo "Iniciando ETL RFB..."
python etl_rfb_dados_g.py
echo "ETL finalizado."
