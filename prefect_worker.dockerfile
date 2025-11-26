FROM python:3.11

# Установка зависимостей для Python 3.11
RUN pip install --upgrade pip
RUN pip install \
    prefect==2.10.12 \
    pydantic==1.10.13 \
    clickhouse-driver==0.2.6 \
    minio==7.1.16 \
    requests==2.31.0 \
    python-telegram-bot==20.7 \
    griffe==0.25.0

RUN pip install anyio==3.7.1
WORKDIR /app
COPY flows/ /app/flows/
RUN mkdir -p /root/.prefect