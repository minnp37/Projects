ARG BUILD_FROM=python:3.12-slim
FROM ${BUILD_FROM}

WORKDIR /app

# Detect package manager and install build deps
RUN if command -v apk >/dev/null 2>&1; then \
        apk add --no-cache \
            python3 \
            py3-pip \
            gcc \
            musl-dev \
            python3-dev; \
    else \
        apt-get update && apt-get install -y \
            gcc \
            python3-dev \
            build-essential \
        && rm -rf /var/lib/apt/lists/*; \
    fi

# Install Python dependencies
RUN pip install --no-cache-dir \
    fastapi \
    uvicorn \
    websockets==10.4 \
    ocpp \
    jinja2 \
    pyyaml \
    aiohttp \
    paho-mqtt

COPY . .

RUN chmod +x run.sh

CMD ["./run.sh"]
