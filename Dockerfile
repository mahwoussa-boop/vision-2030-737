# Streamlit app — build context must be this project root (no `merged/` folder required).
FROM python:3.12-slim-bookworm

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DATA_DIR=/data \
    STREAMLIT_SERVER_PORT=8501 \
    STREAMLIT_SERVER_ADDRESS=0.0.0.0 \
    STREAMLIT_SERVER_HEADLESS=true

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

# Railway / Render set PORT; default 8501 for local docker run.
# نستخدم exec form مع متغير البيئة PORT الذي توفره Railway
# تم استخدام script بسيط لضمان استبدال المتغير بشكل صحيح قبل التشغيل
CMD ["sh", "-c", "STREAMLIT_SERVER_PORT=${PORT:-8501} streamlit run app.py"]
