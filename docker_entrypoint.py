#!/usr/bin/env python3
"""تشغيل Streamlit على Railway رغم STREAMLIT_SERVER_PORT='$PORT' كنص في المتغيرات."""
import os
import sys


def _port() -> int:
    raw = (os.environ.get("PORT") or "").strip() or "8501"
    try:
        p = int(raw)
        if 1 <= p <= 65535:
            return p
    except ValueError:
        pass
    return 8501


def main() -> None:
    p = _port()
    # يطابق سلوك Streamlit ويستبدل أي قيمة خاطئة من لوحة Railway
    os.environ["STREAMLIT_SERVER_PORT"] = str(p)
    os.environ["STREAMLIT_SERVER_ADDRESS"] = "0.0.0.0"
    os.environ["STREAMLIT_SERVER_HEADLESS"] = "true"

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
