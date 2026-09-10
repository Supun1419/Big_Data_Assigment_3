FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    ORDER_SCHEMA_PATH=/app/schemas/order.avsc

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir --requirement requirements.txt

COPY schemas/ ./schemas/
COPY src/ ./src/

CMD ["python", "-m", "src.consumer"]
