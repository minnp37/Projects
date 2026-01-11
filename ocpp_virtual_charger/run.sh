#!/usr/bin/with-contenv bashio

echo "🚀 Starting OCPP Virtual Charger Add-on..."

# 1. Navigate to the application directory
cd /app/app

# 2. Get the Ingress path from Home Assistant 
# This variable tells FastAPI its "new" address inside the HA sidebar
INGRESS_PATH=$(bashio::addon.ingress_entry)
echo "🌐 Ingress is available at: ${INGRESS_PATH}"

# 3. Execute the server
# We use 'exec' to ensure the process handles shutdown signals correctly
# --host 0.0.0.0: Allows connections from outside the container (like port 8001)
# --forwarded-allow-ips: Essential for Ingress to trust the HA proxy
# --root-path: Rewrites URLs so buttons and CSS work inside the HA UI
exec uvicorn virtual_charger:app \
    --host 0.0.0.0 \
    --port 8000 \
    --forwarded-allow-ips="*" \
    --root-path "${INGRESS_PATH}"