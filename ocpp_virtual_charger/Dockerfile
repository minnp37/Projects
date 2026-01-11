ARG BUILD_FROM
FROM $BUILD_FROM

# 1. Install Python and build tools
RUN apk add --no-cache \
    python3 \
    py3-pip \
    gcc \
    musl-dev \
    python3-dev

WORKDIR /app

# 2. Install everything your app needs directly
RUN pip install --no-cache-dir --break-system-packages \
    fastapi \
    uvicorn \
    websockets==10.4 \
    ocpp \
    jinja2 \
    pyyaml \
    aiohttp \
    paho-mqtt

# 3. Copy your code (this includes your /app folder and /run.sh)
COPY . .

RUN chmod a+x run.sh

CMD [ "./run.sh" ]