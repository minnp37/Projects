#!/usr/bin/with-contenv bashio
set -e

echo "🚀 Starting OCPP Virtual Charger Add-on..."

cd /app

# Detect Home Assistant ingress (safe locally)
if command -v bashio >/dev/null 2>&1; then
  INGRESS_PATH=$(bashio::addon.ingress_entry)
else
  INGRESS_PATH=""
fi

echo "🌐 Ingress path: ${INGRESS_PATH}"

exec uvicorn app.virtual_charger:app \
  --host 0.0.0.0 \
  --port 8099 \
  ${INGRESS_PATH:+--root-path "$INGRESS_PATH"} \
  --forwarded-allow-ips="*"
