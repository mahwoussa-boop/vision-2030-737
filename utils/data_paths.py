"""
مسارات التخزين الدائم — Railway Volume وغيرها.

اضبط المتغير DATA_DIR ليطابق مسار تثبيت الـ volume (مثلاً /data).
بدون ذلك يُستخدم /tmp (مناسب لـ Streamlit Cloud؛ يُفقد عند إعادة تشغيل الحاوية).
"""
import os


def get_data_dir() -> str:
    d = (os.environ.get("DATA_DIR") or "").strip()
    if d:
        os.makedirs(d, exist_ok=True)
        return d
    return "/tmp"


def get_data_db_path(filename: str) -> str:
    """مسار ملف قاعدة بيانات داخل مجلد البيانات."""
    return os.path.join(get_data_dir(), filename)
