FROM python:3.11-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends sqlite3 \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir requests==2.32.3

COPY worker.py /app/worker.py
COPY src /app/src

CMD ["python", "/app/worker.py"]
