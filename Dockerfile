FROM python:3.12.12 AS builder

WORKDIR /app

RUN python -m venv .venv

COPY requirements.txt ./
RUN .venv/bin/pip install --no-cache-dir -r requirements.txt

FROM python:3.12.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY --from=builder /app/.venv /app/.venv

COPY . .

EXPOSE 8080

CMD ["/app/.venv/bin/gunicorn", "main:app", "--bind", "0.0.0.0:8080", "--workers", "1", "--worker-class", "gthread", "--threads", "4", "--timeout", "180", "--graceful-timeout", "30", "--keep-alive", "30", "--access-logfile", "-", "--error-logfile", "-", "--capture-output"]
