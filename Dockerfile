FROM python:3.12-slim

# Instalar dependencias de sistema necesarias para Telethon y criptografía
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Variables de entorno para Python
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# Instalar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código de la aplicación
COPY . .

# Crear directorio de descargas con permisos
RUN mkdir -p /app/downloads && chmod 777 /app/downloads

# Exponer el puerto que Fly.io espera
EXPOSE 8080

# Comando de inicio usando gunicorn de forma directa
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "1", "--threads", "4", "--worker-class", "gthread", "--timeout", "120", "main:app"]
