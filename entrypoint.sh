#!/bin/sh
set -e

echo "Iniciando ETL RFB..."
python etl_rfb_dados.py --etl
echo "ETL finalizado."
