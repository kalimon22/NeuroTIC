FROM python:3.11-slim

WORKDIR /app

# Instalar dependencias del sistema requeridas para pycozo (SQLite / build)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos el resto del codigo
COPY . /app/

# Directorio de datos para persistencia de CozoDB embebido
RUN mkdir -p /app/data

# Ojo: si hay un buffer print queremos verlo en tiempo real
ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]
