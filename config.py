"""
config.py - الإعدادات المركزية v26.0
المفاتيح محمية عبر Streamlit Secrets
"""
import json as _json
import os as _os
import tempfile

# جذر المشروع (مجلد config.py) — لا يعتمد على cwd عند streamlit run من مسار آخر
_APP_ROOT = _os.path.dirname(_os.path.abspath(__file__))

# ===== معلومات التطبيق =====
APP_TITLE   = "نظام التسعير الذكي - مهووس"
APP_NAME    = APP_TITLE
APP_VERSION = "v26.0"
APP_ICON    = "🧪"
GEMINI_MODEL = "gemini-2.0-flash"   # النموذج المستقر الموصى به

# ══════════════════════════════════════════════
#  قراءة Secrets بطريقة آمنة 100%
#  تدعم 3 أساليب Streamlit
# ══════════════════════════════════════════════
def _s(key, default=""):
    """
    يقرأ Secret بـ 3 طرق:
    1. st.secrets[key]         الطريقة المباشرة (Streamlit Cloud)
    2. os.environ              Railway Environment Variables
    3. default                 القيمة الافتراضية
    """
    # 1. Railway / os.environ أولاً (يعمل في البناء والتشغيل)
    v = _os.environ.get(key, "")
    if v:
        return v
    # 2. st.secrets (Streamlit Cloud فقط - يُستدعى عند التشغيل)
    try:
        import streamlit as st
        v = st.secrets[key]
        if v is not None:
            return str(v) if not isinstance(v, (list, dict)) else v
    except Exception:
        pass
    return default


def _parse_gemini_keys():
    """
    يجمع مفاتيح Gemini من أي صيغة:
    • GEMINI_API_KEYS = '["key1","key2","key3"]'  (JSON string)
    • GEMINI_API_KEYS = ["key1","key2"]            (TOML array)
    • GEMINI_API_KEY  = "key1"                     (مفتاح واحد)
    • GEMINI_KEY_1 / GEMINI_KEY_2 / ...           (مفاتيح منفصلة)
    """
    keys = []

    # ─── المحاولة 1: GEMINI_API_KEYS (JSON string أو TOML array) ───
    raw = _s("GEMINI_API_KEYS", "")

    if isinstance(raw, list):
        # TOML array مباشرة
        keys = [k for k in raw if k and isinstance(k, str)]
    elif raw and isinstance(raw, str):
        raw = raw.strip()
        # قد تكون JSON string
        if raw.startswith('['):
            try:
                parsed = _json.loads(raw)
                if isinstance(parsed, list):
                    keys = [k for k in parsed if k]
            except Exception:
                # ربما string بدون quotes صحيحة → نظفها
                clean = raw.strip("[]").replace('"','').replace("'",'')
                keys = [k.strip() for k in clean.split(',') if k.strip()]
        elif raw:
            keys = [raw]

    # ─── المحاولة 2: GEMINI_API_KEY (مفتاح واحد) ───
    single = _s("GEMINI_API_KEY", "")
    # Railway/UI أحياناً يحفظان Gemini_API_Key (حالة أحرف مختلفة؛ Linux حساس)
    if not single:
        single = (_os.environ.get("Gemini_API_Key", "") or _os.environ.get("GEMINI_KEY", "")).strip()
    if single and single not in keys:
        keys.append(single)

    # ─── المحاولة 3: مفاتيح منفصلة ───
    for n in ["GEMINI_KEY_1","GEMINI_KEY_2","GEMINI_KEY_3",
              "GEMINI_KEY_4","GEMINI_KEY_5"]:
        k = _s(n, "")
        if k and k not in keys:
            keys.append(k)

    # ─── أسماء بديلة شائعة (Railway / Google AI Studio) ───
    for n in ("GOOGLE_API_KEY", "GOOGLE_AI_API_KEY", "GENERATIVE_AI_API_KEY"):
        k = _s(n, "")
        if k and k not in keys:
            keys.append(k)

    # تنظيف نهائي: مفاتيح Google عادة ≥30 حرفاً؛ الحد الأدنى 12 لتجنب القيم الوهمية
    keys = [k.strip() for k in keys if k and len(k.strip()) >= 12]
    return keys


def get_gemini_api_keys():
    """إعادة قراءة المفاتيح من البيئة (مفيد للعرض بعد تغيير Variables دون إعادة تشغيل العملية)."""
    return _parse_gemini_keys()


def get_openrouter_api_key() -> str:
    """إعادة قراءة المفتاح من البيئة (Railway / Secrets دون إعادة تشغيل العملية)."""
    return _s("OPENROUTER_API_KEY") or _s("OPENROUTER_KEY") or ""


def get_cohere_api_key() -> str:
    return _s("COHERE_API_KEY", "")


def get_apify_token() -> str:
    """
    رمز Apify من Railway / Streamlit Secrets.
    الأسماء المدعومة بالترتيب: APIFY_TOKEN، APIFY_API_TOKEN، API_token (تسمية Railway شائعة).
    """
    return (
        _s("APIFY_TOKEN", "")
        or _s("APIFY_API_TOKEN", "")
        or _s("API_token", "")
    ).strip()


def get_apify_default_actor_id() -> str:
    """
    معرّف الممثل الافتراضي لتشغيله من الواجهة (اختياري).
    الصيغة كما في وثائق REST: username~actorName مثال: immaculate_piccolo~my-actor
    """
    return (_s("APIFY_DEFAULT_ACTOR_ID", "")).strip()


def get_apify_auto_import() -> bool:
    """
    مزامنة تلقائية لآخر تشغيل ناجح → كتالوج المنافسين (comp_catalog).
    إذا لم يُضبط المتغير: يُفعّل تلقائياً عند وجود APIFY_TOKEN و APIFY_DEFAULT_ACTOR_ID معاً.
    عطّل صراحةً بـ APIFY_AUTO_IMPORT=0
    """
    raw = str(_s("APIFY_AUTO_IMPORT", "")).strip().lower()
    if raw in ("0", "false", "no", "off"):
        return False
    if raw in ("1", "true", "yes", "on"):
        return True
    return bool(get_apify_token() and get_apify_default_actor_id())


def get_apify_competitor_label() -> str:
    """اسم المنافس في القاعدة لصفوف Apify (افتراضي: Apify)."""
    v = str(_s("APIFY_COMPETITOR_LABEL", "")).strip()
    return v if v else "Apify"


def apify_auto_import_state_path() -> str:
    """ملف حالة آخر استيراد (داخل data/)."""
    d = _os.path.join(_APP_ROOT, "data")
    try:
        _os.makedirs(d, exist_ok=True)
    except OSError:
        pass
    return _os.path.join(d, "apify_auto_import_state.json")


def get_product_image_base_url() -> str:
    """
    بادئة نطاق المتجر لتحويل روابط الصور النسبية (/storage/...) إلى رابط يعمل في الواجهة.
    مثال: https://mahwous.com أو https://your-store.salla.sa
    """
    return (
        _s("MAHWOUS_IMAGE_ORIGIN", "")
        or _s("PRODUCT_IMAGE_BASE_URL", "")
        or _s("SALLA_STORE_URL", "")
        or _s("STORE_URL", "")
    ).strip().rstrip("/")


def get_our_catalog_basename() -> str:
    """
    ملف كتالوج المتجر داخل data/ (افتراضي mahwous_catalog.csv).
    • MAHWOUS_CATALOG_FILE أو OUR_CATALOG_FILE — اسم الملف فقط أو مع مسار نسبي تحت data/
    • إن لم يُضبط ولم يوجد الافتراضي ووُجد mahwous_store_cleaned.csv يُختار تلقائياً
    """
    explicit = (_s("MAHWOUS_CATALOG_FILE", "") or _s("OUR_CATALOG_FILE", "") or "").strip()
    if explicit:
        return _os.path.basename(explicit.replace("\\", "/"))
    data_dir = _os.path.join(_APP_ROOT, "data")
    primary = "mahwous_catalog.csv"
    fallback = "mahwous_store_cleaned.csv"
    try:
        if _os.path.isfile(_os.path.join(data_dir, primary)):
            return primary
        if _os.path.isfile(_os.path.join(data_dir, fallback)):
            return fallback
    except OSError:
        pass
    return primary


def get_our_catalog_path() -> str:
    """مسار مطلق لملف كتالوج المتجر."""
    return _os.path.join(_APP_ROOT, "data", get_our_catalog_basename())


# ══════════════════════════════════════════════
#  المفاتيح الفعلية (من البيئة / .streamlit/secrets.toml فقط — لا مفاتيح داخل الكود)
#  تُحدَّث عبر _refresh_runtime_secrets() لتفادي الاحتفاظ بقيم قديمة (Stale Globals)
# ══════════════════════════════════════════════
GEMINI_API_KEYS: list[str] = []
GEMINI_API_KEY = ""
OPENROUTER_API_KEY = ""
COHERE_API_KEY = ""
EXTRA_API_KEY = ""


def _refresh_runtime_secrets():
    """تحديث المتغيرات العامة لضمان عدم الاحتفاظ ببيانات قديمة (Stale Globals) في الذاكرة."""
    global GEMINI_API_KEYS, GEMINI_API_KEY, OPENROUTER_API_KEY, COHERE_API_KEY, EXTRA_API_KEY
    GEMINI_API_KEYS = _parse_gemini_keys()
    GEMINI_API_KEY = GEMINI_API_KEYS[0] if GEMINI_API_KEYS else ""
    OPENROUTER_API_KEY = get_openrouter_api_key()
    COHERE_API_KEY = get_cohere_api_key()
    EXTRA_API_KEY = _s("EXTRA_API_KEY", "")


def _apply_scrape_events_ndjson_from_secrets() -> None:
    """تأجيل تعديل بيئة النظام (import-time env mutations) إلى استدعاء صريح."""
    flag = str(_s("MAHWOUS_SCRAPE_EVENTS_NDJSON", "")).strip().lower()
    if flag in ("1", "true", "yes", "on"):
        _os.environ.setdefault("MAHWOUS_SCRAPE_EVENTS_NDJSON", "1")


_refresh_runtime_secrets()
_apply_scrape_events_ndjson_from_secrets()


# ══════════════════════════════════════════════
#  Make Webhooks (يُفضَّل الدوال — تقرأ البيئة/Secrets بعد مزامنة الجلسة في app.py)
# ══════════════════════════════════════════════
def get_webhook_update_prices() -> str:
    return (_s("WEBHOOK_UPDATE_PRICES") or "").strip()


def get_webhook_missing_products() -> str:
    """
    سيناريو «أتمتة التسعير» / إضافة المفقودات في سلة فقط.
    يفضّل WEBHOOK_MISSING_PRODUCTS؛ إن وُجد WEBHOOK_NEW_PRODUCTS قديماً يُستخدم كاحتياط.
    """
    v = (_s("WEBHOOK_MISSING_PRODUCTS") or "").strip()
    if v:
        return v
    return (_s("WEBHOOK_NEW_PRODUCTS") or "").strip()


def get_webhook_new_products() -> str:
    """توافق خلفي — نفس دالة المفقودات."""
    return get_webhook_missing_products()


# توثيق روابط سيناريوهات Make المشتركة (الاستنساخ من المتصفح — الرابط الفعلي للـ Webhook من لوحتك)
MAKE_DOCS_SCENARIO_UPDATE_PRICES = (
    "https://eu2.make.com/public/shared-scenario/9uue7ENfzO5/integration-webhooks-salla"
)
MAKE_DOCS_SCENARIO_PRICING_AUTOMATION = (
    "https://eu2.make.com/public/shared-scenario/UsesKnA62xy/mahwous-pricing-automation-salla"
)

# ══════════════════════════════════════════════
#  كشط (async_scraper.py) — تُقرأ من os.environ على التشغيل
#  • SCRAPER_MAX_CONCURRENT_FETCH (افتراضي 28، حد أعلى 64) — تزيد السرعة؛ خفّضها عند الحظر
#  • SCRAPER_PIPELINE_EVERY — فاصل لقطات المطابقة أثناء الكشط (افتراضي 3؛ 1 = أشد فورية؛ 0 يعطّل الوسيط)
#  • MAHWOUS_UI_LIVE_REFRESH_MS — تبطئة تحديث واجهة Streamlit أثناء الكشط الطويل
#  • MAHWOUS_SCRAPE_UI_MIN_INTERVAL_SEC — أقل فاصل (ثوانٍ) بين كتابات لقطة JSON للتقدم الحي (افتراضي حسب حجم الطابور)
#  استيراد سلة (utils/helpers.py export_missing_products_to_salla_csv_bytes):
#  • SALLA_IMPORT_DEFAULT_CATEGORY — مسار تصنيف افتراضي يطابق categories.csv / لوحة سلة
#  • SALLA_IMPORT_FALLBACK_BRAND — ماركة احتياط عند «غير محدد» (نص كما في brands.csv)
#  • WEBHOOK_UPDATE_PRICES — تعديل أسعار (🔴 أعلى 🟢 أقل ✅ موافق)؛ WEBHOOK_MISSING_PRODUCTS — مفقودات فقط
#  • WEBHOOK_NEW_PRODUCTS — اسم قديم؛ يُقرأ كاحتياط إن لم يُضبط WEBHOOK_MISSING_PRODUCTS
#  AI (engines/ai_engine.py):
#  • OPENROUTER_MODELS — معرّفات نماذج OpenRouter مفصولة بفواصل (تجاوز القائمة الافتراضية)
#  • احذف COHERE_API_KEY من Secrets إذا كان 401 لتقليل الضوضاء (Cohere اختياري)
#  Apify (اختياري — utils/apify_helper.py + utils/apify_sync.py):
#  • APIFY_TOKEN (أو API_token) + APIFY_DEFAULT_ACTOR_ID — مزامنة تلقائية لآخر تشغيل ناجح إلى comp_catalog
#  • APIFY_AUTO_IMPORT — 1/0 (افتراضياً: مفعّل عند وجود الرمز والممثل معاً)
#  • APIFY_COMPETITOR_LABEL — اسم مجموعة المنافس في التحليل (افتراضي Apify)
#  صور كتالوجنا (روابط نسبية من سلة):
#  • MAHWOUS_IMAGE_ORIGIN أو PRODUCT_IMAGE_BASE_URL أو SALLA_STORE_URL — https://نطاق-متجرك
#  • MAHWOUS_CATALOG_FILE — اسم ملف الكتالوج تحت data/ (مثل mahwous_store_cleaned.csv)
# ══════════════════════════════════════════════

# ══════════════════════════════════════════════
#  ألوان
# ══════════════════════════════════════════════
COLORS = {
    "raise": "#dc3545", "lower": "#ffc107", "approved": "#28a745",
    "missing": "#007bff", "review": "#ff9800", "primary": "#6C63FF",
}

# ══════════════════════════════════════════════
#  إعدادات المطابقة
# ══════════════════════════════════════════════
MATCH_THRESHOLD    = 85
HIGH_CONFIDENCE    = 95
REVIEW_THRESHOLD   = 75
PRICE_TOLERANCE    = 5
MIN_MATCH_SCORE    = MATCH_THRESHOLD
HIGH_MATCH_SCORE   = HIGH_CONFIDENCE
PRICE_DIFF_THRESHOLD = PRICE_TOLERANCE

# ══════════════════════════════════════════════
#  فلاتر المنتجات
# ══════════════════════════════════════════════
REJECT_KEYWORDS = [
    "sample","عينة","عينه","decant","تقسيم","تقسيمة",
    "split","miniature","0.5ml","1ml","2ml","3ml",
    "vial","سمبل",
]
TESTER_KEYWORDS = ["tester","تستر","تيستر"]
SET_KEYWORDS    = ["set","gift set","طقم","مجموعة","coffret"]

# ══════════════════════════════════════════════
#  العلامات التجارية
# ══════════════════════════════════════════════
KNOWN_BRANDS = [
    "Dior","Chanel","Gucci","Tom Ford","Versace","Armani","YSL","Prada",
    "Burberry","Givenchy","Hermes","Creed","Montblanc","Calvin Klein",
    "Hugo Boss","Dolce & Gabbana","Valentino","Bvlgari","Cartier","Lancome",
    "Jo Malone","Amouage","Rasasi","Lattafa","Arabian Oud","Ajmal",
    "Al Haramain","Afnan","Armaf","Nishane","Xerjoff","Parfums de Marly",
    "Initio","Byredo","Le Labo","Mancera","Montale","Kilian","Roja",
    "Carolina Herrera","Jean Paul Gaultier","Narciso Rodriguez",
    "Paco Rabanne","Mugler","Chloe","Coach","Michael Kors","Ralph Lauren",
    "Maison Margiela","Memo Paris","Penhaligons","Serge Lutens","Diptyque",
    "Frederic Malle","Francis Kurkdjian","Floris","Clive Christian",
    "Ormonde Jayne","Zoologist","Tauer","Lush","The Different Company",
    "Missoni","Juicy Couture","Moschino","Dunhill","Bentley","Jaguar",
    "Boucheron","Chopard","Elie Saab","Escada","Ferragamo","Fendi",
    "Kenzo","Lacoste","Loewe","Rochas","Roberto Cavalli","Tiffany",
    "Van Cleef","Azzaro","Banana Republic","Benetton","Bottega Veneta",
    "Celine","Dsquared2","Ed Hardy","Elizabeth Arden","Ermenegildo Zegna",
    "Swiss Arabian","Ard Al Zaafaran","Nabeel","Asdaaf","Maison Alhambra",
    "لطافة","العربية للعود","رصاصي","أجمل","الحرمين","أرماف",
    "أمواج","كريد","توم فورد","ديور","شانيل","غوتشي","برادا",
    "ميسوني","جوسي كوتور","موسكينو","دانهيل","بنتلي",
    "كينزو","لاكوست","فندي","ايلي صعب","ازارو",
    "Guerlain","Givenchy","Sisley","Issey Miyake","Davidoff","Mexx",
]

# ══════════════════════════════════════════════
#  استبدالات التطبيع
# ══════════════════════════════════════════════
WORD_REPLACEMENTS = {
    'او دو بارفان':'edp','أو دو بارفان':'edp','او دي بارفان':'edp',
    'او دو تواليت':'edt','أو دو تواليت':'edt','او دي تواليت':'edt',
    'مل':'ml','ملي':'ml',
    'سوفاج':'sauvage','ديور':'dior','شانيل':'chanel',
    'توم فورد':'tom ford','أرماني':'armani','غيرلان':'guerlain',
}

# ══════════════════════════════════════════════
#  إعدادات الأتمتة الذكية v26.0
# ══════════════════════════════════════════════
AUTOMATION_RULES_DEFAULT = [
    {
        "name": "خفض السعر تلقائياً",
        "enabled": True,
        "condition": "our_price > comp_price",
        "min_diff": 10,       # فرق أدنى بالريال لتفعيل القاعدة
        "action": "undercut",  # خفض ليصبح أقل من المنافس
        "undercut_amount": 1,  # أقل بكم ريال
        "min_match_score": 90, # حد أدنى لنسبة التطابق
        "max_loss_pct": 15,    # أقصى نسبة خسارة مقبولة من سعر التكلفة
    },
    {
        "name": "رفع السعر عند فرصة ربح",
        "enabled": True,
        "condition": "our_price < comp_price",
        "min_diff": 15,
        "action": "raise_to_match",
        "margin_below": 5,     # أقل من المنافس بكم ريال
        "min_match_score": 90,
    },
    {
        "name": "إبقاء السعر إذا تنافسي",
        "enabled": True,
        "condition": "abs(our_price - comp_price) <= threshold",
        "threshold": 10,
        "action": "keep",
        "min_match_score": 85,
    },
]

# جدولة البحث الدوري (بالدقائق)
AUTO_SEARCH_INTERVAL_MINUTES = 60 * 6   # كل 6 ساعات
AUTO_PUSH_TO_MAKE = False               # إرسال تلقائي لـ Make.com (يتطلب تفعيل يدوي)
AUTO_DECISION_CONFIDENCE = 92           # حد الثقة للقرار التلقائي (تسعير/رفع-خفض)
# حاجز المفقودات: تطابق نصي مع كتالوجنا (token_set_ratio) — يُستبعد عند ≥88%
SMART_MISSING_FUZZ_THRESHOLD = 88
# تحقق AI لقسم المراجعة — واقعي مع مخرجات verify_match (غالباً 65–90)
REVIEW_VERIFY_MIN_CONFIDENCE = 72

# ══════════════════════════════════════════════
#  أقسام التطبيق (v26.0 — مع لوحة الأتمتة)
# ══════════════════════════════════════════════
SECTIONS = [
    "📊 لوحة التحكم",
    "📂 رفع الملفات",
    "➕ منتج سريع",
    "🔴 سعر أعلى",
    "🟢 سعر أقل",
    "✅ موافق عليها",
    "🔍 منتجات مفقودة",
    "⚠️ تحت المراجعة",
    "✔️ تمت المعالجة",
    "🤖 الذكاء الصناعي",
    "⚡ أتمتة Make",
    "🔄 الأتمتة الذكية",
    "⚙️ الإعدادات",
    "📜 السجل",
]
SIDEBAR_SECTIONS = SECTIONS
PAGES_PER_TABLE  = 25
# مسار SQLite — نفس الاسم في كل الوحدات؛ temp يعمل على Windows وLinux وStreamlit Cloud
DB_PATH = _os.path.join(tempfile.gettempdir(), "pricing_v18.db")

# قائمة المنافسين الافتراضية للكشط — يُحمَّل من الملف؛ يمكن تعديل JSON دون المساس بالكود
PRESET_COMPETITORS_PATH = _os.path.join(_APP_ROOT, "data", "preset_competitors.json")
# إن فُقد الملف على السيرفر (Docker/Volume) تُستخدم هذه القائمة — نفس محتوى data/preset_competitors.json
PRESET_COMPETITORS_FALLBACK: list[dict] = [
    {"name": "سعيد صلاح", "store_url": "https://saeedsalah.com/", "sitemap_url": "https://saeedsalah.com/sitemap.xml"},
    {"name": "فانيلا", "store_url": "https://vanilla.sa/", "sitemap_url": "https://vanilla.sa/sitemap.xml"},
    {"name": "سارا ميكب", "store_url": "https://sara-makeup.com/", "sitemap_url": "https://sara-makeup.com/sitemap.xml"},
    {"name": "خبير العطور", "store_url": "https://alkhabeershop.com/", "sitemap_url": "https://alkhabeershop.com/sitemap.xml"},
    {"name": "قولدن سنت", "store_url": "https://www.goldenscent.com/", "sitemap_url": "https://www.goldenscent.com/sitemap.xml"},
    {"name": "لي سانتو", "store_url": "https://leesanto.com/", "sitemap_url": "https://leesanto.com/sitemap.xml"},
    {"name": "آزال", "store_url": "https://azalperfume.com/", "sitemap_url": "https://azalperfume.com/sitemap.xml"},
    {"name": "كاندي نيش", "store_url": "https://candyniche.com/", "sitemap_url": "https://candyniche.com/sitemap.xml"},
    {"name": "الفاخرة للنيش", "store_url": "https://luxuryperfumesnish.com/", "sitemap_url": "https://luxuryperfumesnish.com/sitemap.xml"},
    {"name": "حنان العطور", "store_url": "https://hanan-store55.com/", "sitemap_url": "https://hanan-store55.com/sitemap.xml"},
    {"name": "اريج امواج", "store_url": "https://areejamwaj.com/", "sitemap_url": "https://areejamwaj.com/sitemap.xml"},
    {"name": "نايس ون", "store_url": "https://niceonesa.com/", "sitemap_url": "https://niceonesa.com/sitemap.xml"},
    {"name": "سيفورا", "store_url": "https://www.sephora.me/sa-ar", "sitemap_url": "https://www.sephora.me/sa-ar/sitemap.xml"},
    {"name": "وجوه", "store_url": "https://www.faces.sa/ar", "sitemap_url": "https://www.faces.sa/ar/sitemap.xml"},
    {"name": "نيش", "store_url": "https://niche.sa/", "sitemap_url": "https://niche.sa/sitemap.xml"},
    {"name": "عالم جيفنشي", "store_url": "https://worldgivenchy.com/", "sitemap_url": "https://worldgivenchy.com/sitemap.xml"},
    {"name": "ساره ستور", "store_url": "https://sarahmakeup37.com/", "sitemap_url": "https://sarahmakeup37.com/sitemap.xml"},
    {"name": "اروماتيك كلاود", "store_url": "https://aromaticcloud.com/", "sitemap_url": "https://aromaticcloud.com/sitemap.xml"},
]
