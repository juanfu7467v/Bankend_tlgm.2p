FROM python:3.12-slim

# Evitar que apt-get se detenga por prompts
ENV DEBIAN_FRONTEND=noninteractive

WORKDIR /app

# Variables de entorno críticas
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    PORT=8080

# Instalamos dependencias primero para aprovechar la caché de capas
COPY requirements.txt .
RUN pip install --no-cache-dir -U pip && \
    pip install --no-cache-dir -r requirements.txt

# Copiamos el resto del código
COPY . .

# Asegurar directorios
RUN mkdir -p /app/downloads

# Exponer puerto
EXPOSE 8080

# Comando optimizado
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "1", "--threads", "4", "--worker-class", "gthread", "--timeout", "120", "main:app"]
