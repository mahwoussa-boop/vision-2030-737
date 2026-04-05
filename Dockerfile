# Streamlit app — build context must be this project root (no `merged/` folder required).
FROM python:3.12-slim-bookworm

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DATA_DIR=/data

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# إنشاء مجلد البيانات والتأكد من صلاحيات الكتابة
RUN mkdir -p /data && chmod 777 /data

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# الاعتماد على الإعدادات الموجودة في .streamlit/config.toml
# Railway ستقوم بتوجيه الترافيك تلقائياً للمنفذ الذي تفتحه الحاوية
CMD ["streamlit", "run", "app.py"]
