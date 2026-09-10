FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    ORDER_SCHEMA_PATH=/app/schemas/order.avsc

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir --requirement requirements.txt

COPY schemas/ ./schemas/
COPY src/ ./src/

FROM base AS test

COPY requirements-dev.txt ./
RUN pip install --no-cache-dir --requirement requirements-dev.txt
COPY tests/ ./tests/

CMD ["pytest", "-q"]

FROM base AS runtime

CMD ["python", "-m", "src.consumer"]
