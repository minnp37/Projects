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
            python3-dev \
            libffi-dev \
            bash; \
    else \
        apt-get update && apt-get install -y \
            gcc \
            python3-dev \
            build-essential \
            libffi-dev \
            bash \
        && rm -rf /var/lib/apt/lists/*; \
    fi

# Create a virtual environment for Python packages
RUN python3 -m venv /opt/venv \
    && /opt/venv/bin/pip install --upgrade pip \
    && /opt/venv/bin/pip install --no-cache-dir \
        fastapi \
        uvicorn \
        websockets==10.4 \
        ocpp==2.1.0 \
        jinja2 \
        pyyaml \
        aiohttp \
        paho-mqtt

# Make the venv Python the default
ENV PATH="/opt/venv/bin:$PATH"

# Copy your add-on code
COPY . .

# Make run.sh executable
RUN chmod +x run.sh

# Ensure /data exists and declare as volume for persistence
RUN mkdir -p /data
VOLUME /data

# Start the add-on
CMD ["./run.sh"]
