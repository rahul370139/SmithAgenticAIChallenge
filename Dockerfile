# AI Cargo Monitor — FastAPI backend for Hostinger VPS
# Same pattern as Pathwise: Dockerized API behind nginx, frontend stays on Vercel.

FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PORT=8000

WORKDIR /app

# System deps for xgboost / scientific stack
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps (Linux-friendly; includes CPU torch for compliance embeddings)
COPY requirements.docker.txt .
RUN pip install --upgrade pip \
    && pip install -r requirements.docker.txt

# App code + trained artifacts (scored CSV + XGBoost model)
COPY backend/ ./backend/
COPY orchestrator/ ./orchestrator/
COPY src/ ./src/
COPY tools/ ./tools/
COPY streaming/ ./streaming/
COPY artifacts/ ./artifacts/
COPY data/ ./data/
COPY pipeline.py ./pipeline.py

# Non-root user
RUN useradd -m -u 10001 appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD curl -fsS "http://127.0.0.1:${PORT}/api/windows?limit=1" || exit 1

CMD ["sh", "-c", "uvicorn backend.app:app --host 0.0.0.0 --port ${PORT}"]
