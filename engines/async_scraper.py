"""
scrapers/async_scraper.py — Shim (توافق رجعي)
═══════════════════════════════════════════════
المصدر الحقيقي: engines/async_scraper.py
هذا الملف مجرد جسر للتوافق مع أي import قديم يستخدم scrapers.async_scraper
جميع الوظائف والمتغيرات متاحة عبر هذا الملف بدون تغيير السلوك.
"""
from engines.async_scraper import *  # noqa: F401, F403
from engines.async_scraper import (  # noqa: F401 — explicit للـ linters
    run_scraper,
    main,
    extract_product,
    fetch_product,
    Progress,
    COMPETITORS_FILE,
    OUTPUT_CSV,
    PROGRESS_FILE,
    LASTMOD_FILE,
    CSV_COLS,
)

# نقطة دخول مستقلة للتوافق مع التشغيل المباشر: python scrapers/async_scraper.py
if __name__ == "__main__":
    main()
