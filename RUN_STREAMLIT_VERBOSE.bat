@echo off
REM تشغيل التطبيق مع تسجيل أوضح للتحذيرات — انقر نقراً مزدوجاً على هذا الملف
chcp 65001 >nul
cd /d "%~dp0"

REM يمنع أخطاء ترميز العربية في الطرفية
set PYTHONIOENCODING=utf-8

REM يعرض تحذيرات بايثون (مثل soon-deprecated) بدل إخفائها
set PYTHONWARNINGS=default

echo.
echo === مهووس — وضع تسجيل أوضح ===
echo راقب هذه النافذة عند حدوث خطأ؛ انسخ Traceback كاملاً إذا احتجت المساعدة.
echo لإيقاف الخادم: Ctrl+C
echo.

python -m streamlit run app.py

echo.
pause
