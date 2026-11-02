#!/bin/bash
# Para el script de Python y los contenedores de Docker.
# Los datos de InfluxDB, PostgreSQL y Grafana se conservan en volumenes de Docker.
# Para borrarlo todo:  docker compose down -v
DIR="$(cd "$(dirname "$0")" && pwd)"

if pkill -f "trenes.py"; then
    echo "trenes.py detenido."
else
    echo "trenes.py no estaba en ejecucion."
fi

docker compose -f "$DIR/../docker-compose.yml" down
echo "Todo parado. La ventana de Terminal del script puedes cerrarla."
