# syntax=docker/dockerfile:1

# ---------- Stage 1: build the virtualenv (build tooling never reaches runtime) ----------
FROM python:3.12-slim AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /build
# Copy only requirements first so this layer is cached until dependencies change.
COPY requirements.txt .
RUN pip install -r requirements.txt


# ---------- Stage 2: slim runtime image ----------
FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH" \
    FASTAPI_HOST=0.0.0.0 \
    FASTAPI_PORT=8000

# curl is only needed for the container health check.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system app \
    && useradd --system --gid app --home-dir /app --shell /usr/sbin/nologin app

COPY --from=builder /opt/venv /opt/venv

WORKDIR /app
COPY app ./app
COPY tests ./tests
COPY pytest.ini .

USER app
EXPOSE 8000

# `exec` makes uvicorn PID 1 so it receives SIGTERM and shuts down gracefully.
CMD ["sh", "-c", "exec uvicorn app.main:app --host ${FASTAPI_HOST} --port ${FASTAPI_PORT} --no-access-log"]
