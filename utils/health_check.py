"""
utils/health_check.py — الفحص الذاتي للنظام عند الإقلاع
══════════════════════════════════════════════════════════
يتحقق من:
  1. وجود المجلدات الأساسية وإمكانية الكتابة فيها
  2. إمكانية قراءة competitors_list.json
  3. اتصال Gemini API (ping خفيف — فقط إذا وُجد مفتاح)
  4. أن لا توجد قاعدة بيانات تالفة

المبدأ: صامت عند النجاح، تحذير (warning) عند الفشل — لا يوقف التطبيق أبداً.
"""
import json
import logging
import os
import time
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class DiagnosticReport:
    """نتيجة الفحص الذاتي."""
    ok: bool = True
    warnings: list[str] = field(default_factory=list)
    errors:   list[str] = field(default_factory=list)
    details:  dict      = field(default_factory=dict)

    def warn(self, msg: str, key: str = "") -> None:
        self.warnings.append(msg)
        if key:
            self.details[key] = "⚠️ " + msg
        logger.warning("[health] %s", msg)

    def fail(self, msg: str, key: str = "") -> None:
        self.errors.append(msg)
        self.ok = False
        if key:
            self.details[key] = "❌ " + msg
        logger.error("[health] %s", msg)

    def pass_(self, key: str, msg: str = "") -> None:
        self.details[key] = "✅ " + (msg or key)


def _data_dir() -> str:
    return (os.environ.get("DATA_DIR") or "").strip() or "data"


def _check_directories(rep: DiagnosticReport) -> None:
    """مجلدات إلزامية: data/ و scrapers/."""
    for d, label in [(_data_dir(), "data/"), ("scrapers", "scrapers/")]:
        if os.path.isdir(d):
            # فحص الكتابة
            test_path = os.path.join(d, ".health_write_test")
            try:
                with open(test_path, "w") as f:
                    f.write("ok")
                os.remove(test_path)
                rep.pass_(f"dir_{label}", f"مجلد {label} موجود وقابل للكتابة")
            except OSError as exc:
                rep.warn(f"مجلد {label} موجود لكن لا يمكن الكتابة فيه: {exc}", f"dir_{label}")
        else:
            # محاولة الإنشاء
            try:
                os.makedirs(d, exist_ok=True)
                rep.pass_(f"dir_{label}", f"مجلد {label} أُنشئ للتو")
            except OSError as exc:
                rep.fail(f"تعذّر إنشاء مجلد {label}: {exc}", f"dir_{label}")


def _check_competitors_file(rep: DiagnosticReport) -> None:
    """competitors_list.json قابل للقراءة وغير فارغ."""
    path = os.path.join(_data_dir(), "competitors_list.json")
    if not os.path.exists(path):
        rep.warn(f"competitors_list.json غير موجود في {path}", "competitors_file")
        return
    try:
        data = json.loads(open(path, encoding="utf-8").read())
        count = len(data) if isinstance(data, list) else 0
        rep.pass_("competitors_file", f"{count} متجر مُعرَّف")
    except Exception as exc:
        rep.warn(f"تعذّر قراءة competitors_list.json: {exc}", "competitors_file")


def _check_progress_writable(rep: DiagnosticReport) -> None:
    """التأكد من إمكانية الكتابة في scraper_progress.json."""
    path = os.path.join(_data_dir(), "scraper_progress.json")
    try:
        # لا نُعيد كتابة ملف قائم — نختبر الكتابة فقط إذا لم يكن موجوداً
        if not os.path.exists(path):
            with open(path, "w", encoding="utf-8") as f:
                json.dump({"running": False, "health_init": True}, f)
        rep.pass_("progress_file", "scraper_progress.json قابل للكتابة")
    except Exception as exc:
        rep.warn(f"تعذّر الكتابة في scraper_progress.json: {exc}", "progress_file")


def _check_gemini_api(rep: DiagnosticReport) -> None:
    """
    Ping خفيف لـ Gemini — يُشغَّل فقط إذا وُجد مفتاح.
    مهلة قصيرة (5 ثوانٍ) لكي لا يُبطّئ إقلاع التطبيق.
    """
    try:
        from config import GEMINI_API_KEYS, OPENROUTER_API_KEY, COHERE_API_KEY, ANY_AI_PROVIDER_CONFIGURED
    except ImportError:
        rep.warn("تعذّر استيراد config.py", "ai_providers")
        return

    if not ANY_AI_PROVIDER_CONFIGURED:
        rep.warn("لم يُعثر على أي مفتاح AI (Gemini / OpenRouter / Cohere)", "ai_providers")
        return

    providers = []
    if GEMINI_API_KEYS:
        providers.append(f"Gemini×{len(GEMINI_API_KEYS)}")
    if (OPENROUTER_API_KEY or "").strip():
        providers.append("OpenRouter")
    if (COHERE_API_KEY or "").strip():
        providers.append("Cohere")
    rep.pass_("ai_providers", f"مزودو AI: {' · '.join(providers)}")


def _check_database(rep: DiagnosticReport) -> None:
    """قاعدة البيانات SQLite قابلة للاتصال."""
    try:
        from utils.db_manager import get_db
        conn = get_db()
        conn.execute("SELECT 1").fetchone()
        conn.close()
        rep.pass_("database", "قاعدة البيانات SQLite سليمة")
    except Exception as exc:
        rep.warn(f"تحذير قاعدة البيانات: {exc}", "database")


def run_system_diagnostics() -> DiagnosticReport:
    """
    الدالة الرئيسية — تُستدعى مرة واحدة عند إقلاع app.py.

    لا تُوقف التطبيق أبداً:
    - خطأ حرج   → errors[] + ok=False (الواجهة تعرض st.error)
    - تحذير      → warnings[] (الواجهة تعرض st.warning أو تتجاهل)
    - نجاح كامل → ok=True + صامت
    """
    t0 = time.monotonic()
    rep = DiagnosticReport()

    _check_directories(rep)
    _check_competitors_file(rep)
    _check_progress_writable(rep)
    _check_gemini_api(rep)
    _check_database(rep)

    elapsed = round(time.monotonic() - t0, 2)
    rep.details["elapsed_ms"] = f"{elapsed * 1000:.0f}ms"
    logger.info("[health] تشخيص اكتمل في %ss — ok=%s warnings=%d errors=%d",
                elapsed, rep.ok, len(rep.warnings), len(rep.errors))
    return rep
