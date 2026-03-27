# Usamos una versión estable y ligera
FROM python:3.11-slim

# Directorio de trabajo
WORKDIR /app

# Instalar dependencias del sistema necesarias para librerías como Telethon/Aiohttp
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copiar requerimientos e instalar (Asegúrate que el archivo se llame exactamente requirements.txt)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar todo el código del proyecto
COPY . .

# Exponer el puerto que usa Fly.io
EXPOSE 8080

# Comando de inicio usando python -m para mayor compatibilidad
CMD ["python", "-m", "gunicorn", "main:app", "--bind", "0.0.0.0:8080", "--workers", "1", "--threads", "8", "--worker-class", "gthread", "--timeout", "120"]
