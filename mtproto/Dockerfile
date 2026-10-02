# ---- build stage: compile TgCrypto (C extension, no prebuilt 3.12 wheel) ----
FROM python:3.12-slim AS builder

ENV PIP_NO_CACHE_DIR=1
RUN apt-get update \
 && apt-get install -y --no-install-recommends gcc libc6-dev \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY requirements.txt ./
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ---- runtime stage: slim image, no compiler shipped ----
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DOWNLOAD_DIR=/tmp/downloads \
    SESSION_DIR=/tmp/session

COPY --from=builder /install /usr/local

WORKDIR /app
COPY config.py resolvers.py downloader.py health.py bot.py ./

RUN mkdir -p /tmp/downloads /tmp/session

EXPOSE 8000

CMD ["python", "bot.py"]
