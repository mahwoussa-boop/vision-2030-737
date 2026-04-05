#!/usr/bin/env python3
"""
تشغيل Streamlit على Railway.
Railway قد يضع STREAMLIT_SERVER_PORT على النص الحرفي '$PORT' — نزيله ثم نمرّر المنفذ رقماً.
"""
import os


def _port() -> int:
    raw = (os.environ.get("PORT") or "").strip() or "8501"
    try:
        p = int(raw)
        if 1 <= p <= 65535:
            return p
    except ValueError:
        pass
    return 8501


def _strip_broken_streamlit_server_env() -> None:
    for key in list(os.environ):
        if key.startswith("STREAMLIT_SERVER_"):
            os.environ.pop(key, None)


def main() -> None:
    p = _port()
    _strip_broken_streamlit_server_env()
    # لا نضبط STREAMLIT_SERVER_* هنا — الاعتماد على سطر الأوامر فقط يتفادى تعارض القيم النصية '$PORT'
    os.execvp(
        "streamlit",
        [
            "streamlit",
            "run",
            "app.py",
            "--server.port",
            str(p),
            "--server.address",
            "0.0.0.0",
            "--server.headless",
            "true",
        ],
    )


if __name__ == "__main__":
    main()
