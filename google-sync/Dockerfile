FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY pyproject.toml poetry.lock README.md /app/
COPY src /app/src

RUN pip install --no-cache-dir .

CMD ["python", "-m", "src.google.scheduler"]
