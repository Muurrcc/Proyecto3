#!/bin/bash
# Arranca Docker (Mosquitto, Telegraf, InfluxDB, PostgreSQL, Flask, Grafana, pgAdmin, Chronograf) y abre
# una ventana de Terminal con el script que consulta los feeds de Renfe.
DIR="$(cd "$(dirname "$0")" && pwd)"

echo "Arrancando contenedores..."
docker compose -f "$DIR/../docker-compose.yml" up -d --build || { echo "Docker no esta en marcha."; exit 1; }

echo -n "Esperando a Mosquitto (1883)"
for _ in $(seq 1 30); do
    nc -z localhost 1883 2>/dev/null && break
    echo -n "."; sleep 1
done
echo

if pgrep -f "trenes.py" >/dev/null; then
    echo "trenes.py ya esta en ejecucion, no lo abro otra vez."
else
    osascript <<APPLESCRIPT
tell application "Terminal"
    activate
    do script "cd '$DIR/../api' && python3 trenes.py"
end tell
APPLESCRIPT
    echo "trenes.py abierto en una ventana nueva."
fi

echo "Grafana:  http://localhost:3001   (admin / admin)"
echo "Flask:    http://localhost:5002"
echo "Postgres: localhost:5433  (practica3 / practica3, base practica3)"
echo "pgAdmin:  http://localhost:5050   (PostgreSQL, sin login)"
echo "Chronograf: http://localhost:8888 (InfluxDB, sin login)"
