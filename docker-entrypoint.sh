#!/bin/sh
set -e
# لوحة Railway أو قوالب قد تضبط STREAMLIT_SERVER_PORT على النص الحرفي "$PORT" — Streamlit يرفض التشغيل
unset STREAMLIT_SERVER_PORT 2>/dev/null || true

# Railway يوفّر PORT رقماً؛ محلياً أو بدون PORT نستخدم 8501
_PORT="${PORT:-8501}"
# رفض إن كان غير رقمي (مثلاً بقيت القيمة '$PORT' كسلسلة في متغير آخر)
case "$_PORT" in
  *[!0-9]*) _PORT=8501 ;;
esac

exec streamlit run app.py \
  --server.port="$_PORT" \
  --server.address=0.0.0.0 \
  --server.headless=true
