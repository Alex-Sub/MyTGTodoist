FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir requests==2.32.3

COPY worker.py /app/worker.py
COPY runtime_server.py /app/runtime_server.py
COPY runtime_handlers.py /app/runtime_handlers.py
COPY shared_runtime.py /app/shared_runtime.py
COPY src /app/src

CMD ["python", "/app/worker.py"]
