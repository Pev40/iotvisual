# Dockerfile para el analizador de datos
FROM python:3.11-slim

# Metadata
LABEL maintainer="freefall-project"
LABEL description="Arduino Freefall Data Analyzer with Machine Learning"

# Variables de entorno
ENV PYTHONUNBUFFERED=1
ENV DEBIAN_FRONTEND=noninteractive

# Directorio de trabajo
WORKDIR /app

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    postgresql-client \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Crear usuario no-root
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Copiar requirements
COPY requirements.txt .

# Actualizar pip y instalar dependencias Python como root (antes de cambiar de usuario)
RUN pip install --root-user-action=ignore --upgrade pip && \
    pip install --root-user-action=ignore --no-cache-dir --no-warn-script-location -r requirements.txt

# Copiar código fuente
COPY conector.py .

# Crear directorios y dar permisos
RUN mkdir -p /app/results /app/logs && chown -R appuser:appuser /app

# Cambiar a usuario no-root
USER appuser

# Exponer puerto
EXPOSE 5000

# Comando por defecto
CMD ["python", "conector.py"]
