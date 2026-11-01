#!/bin/bash
# Starts the Virtual Car Charger inside the add-on container.
# No Home Assistant base image or bashio needed: Home Assistant's ingress proxy strips its path prefix and the
# web page only uses relative URLs, so no --root-path is required. Settings are read from /data/options.json.
set -e

cd /app
echo "Starting Virtual Car Charger add-on..."

exec uvicorn app.virtual_charger:app \
  --host 0.0.0.0 \
  --port 8099 \
  --ws none \
  --forwarded-allow-ips="*"
