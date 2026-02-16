FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir requests==2.32.3

COPY bot.py /app/bot.py

CMD ["python", "/app/bot.py"]
