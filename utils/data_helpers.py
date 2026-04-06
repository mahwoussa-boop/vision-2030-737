"""
دوال مساعدة خالصة (بدون واجهة ولا session_state) — تجهيز البيانات والنصوص.
"""
import json
import os
import re
from datetime import datetime

import pandas as pd
from rapidfuzz import process as rf_proc, fuzz as rf_fuzz

# تسلسل شائع في سلة/إكسيل: ...jpg,https://...
_AFTER_EXT_COMMA_HTTP = re.compile(
    r"\.(?:webp|jpg|jpeg|png|gif|avif)\s*[,،]\s*https?://",
    re.I,
)


def _looks_like_several_image_urls(s: str) -> bool:
    """True فقط عندما يُرجّح أن النص يضم أكثر من رابط (لا نلمس رابط المنافس بفاصلة داخل ?query)."""
    if not s or ("http://" not in s and "https://" not in s):
        return False
    n = s.count("http://") + s.count("https://")
    if n > 1:
        return True
    return bool(_AFTER_EXT_COMMA_HTTP.search(s))

# حقول وسائط قد تُحفظ كـ NaN — لا تُستبدل بالصفر
_MEDIA_KEYS_EMPTY_ON_NA = frozenset({
    "صورة_منتجنا", "رابط_منتجنا", "صورة_المنتج", "رابط_المنتج",
    "رابط_المنافس",
    "صورة المنتج", "رابط المنتج", "صوره المنتج", "الرابط", "رابط",
})


def first_image_url_string(s: str) -> str:
    """
    أرجع أول رابط http يبدو ملف صورة، مع دعم استثنائي لروابط Cloudflare/Salla CDN
    التي تحتوي على فواصل في مسارها (مثل fit=scale-down,width=500).
    """
    s = (s or "").strip()
    if not s: return ""

    # فصل الروابط المتعددة المدمجة بمسافة أو فاصلة دون تدمير روابط CDN
    if "http" in s.lower():
        start = s.lower().find("http")
        next_http = s.lower().find("http", start + 4)
        if next_http > 0:
            s = s[:next_http].rstrip(",، \t\n\r")

    if "cdn-cgi/image" in s or "cdn.salla" in s:
        inner = re.search(r'cdn-cgi/image/[^/]+/(https?://[^\s<>"\']+)', s)
        if inner:
            return inner.group(1).rstrip(".,;)>]")
        m = re.search(r"(https?://[^\s\"\'<>]+)", s)
        return m.group(1).rstrip(".,;)>]") if m else s.split()[0]

    m = re.search(r"(https?://[^\s<>\"\'\,\u060c؛;]+?\.(?:webp|jpg|jpeg|png|gif|avif))", s, re.I)
    if m: return m.group(1).rstrip(".,;)>]")

    m2 = re.search(r"(https?://[^\s\"\'<>]+)", s)
    return m2.group(1).rstrip(".,;)>]") if m2 else s.split()[0]


def _strip_media_val(v):
    if v is None:
        return ""
    try:
        if isinstance(v, float) and pd.isna(v):
            return ""
        if pd.isna(v) and not isinstance(v, (list, dict, str)):
            return ""
    except (TypeError, ValueError):
        pass
    s = str(v).strip()
    if not s or s.lower() in ("nan", "none", "0", "<na>"):
        return ""
    return s


def normalize_result_media_keys(row: dict) -> None:
    """يوحّد صورة/رابط منتجنا تحت المفتاحين المعتمدين في الواجهة والمحرك."""
    if not row:
        return
    if not _strip_media_val(row.get("صورة_منتجنا")):
        for alt in ("صورة_المنتج", "صورة المنتج", "صوره المنتج"):
            if alt in row:
                v = _strip_media_val(row.get(alt))
                if v:
                    row["صورة_منتجنا"] = v
                    break
    if not _strip_media_val(row.get("رابط_منتجنا")):
        for alt in ("رابط_المنتج", "رابط المنتج", "الرابط", "رابط"):
            if alt in row:
                v = _strip_media_val(row.get(alt))
                if v:
                    row["رابط_منتجنا"] = v
                    break


def row_media_urls_from_analysis(row) -> tuple:
    """
    صورة منتجنا + صورة المنافس الرئيسي من صف نتيجة (Series أو dict).
    يعتمد على مفتاحي صورة_منتجنا وجميع_المنافسين بعد التطبيع.
    """
    if row is None:
        return ("", "")
    d = row.to_dict() if hasattr(row, "to_dict") else dict(row)
    normalize_result_media_keys(d)
    our_img = first_image_url_string(str(d.get("صورة_منتجنا", "") or "").strip())
    comp_img = first_image_url_string(str(d.get("صورة_المنافس", "") or "").strip())
    all_c = d.get("جميع_المنافسين", d.get("جميع المنافسين", [])) or []
    if isinstance(all_c, str):
        try:
            all_c = json.loads(all_c)
        except Exception:
            all_c = []
    if not isinstance(all_c, list):
        all_c = []
    comp_name = str(d.get("منتج_المنافس", "—"))
    for c in all_c:
        if str(c.get("name", "")).strip() == str(comp_name).strip():
            # لا نكتب فوق الصورة الصحيحة الموجودة بقيمة فارغة
            candidate_img = first_image_url_string(
                str(c.get("image_url") or c.get("thumb") or c.get("صورة_المنافس") or "").strip()
            )
            if candidate_img:
                comp_img = candidate_img
            break
    # fallback: أول منافس في القائمة إن لم تُوجد صورة بعد
    if not comp_img and all_c:
        comp_img = first_image_url_string(
            str(all_c[0].get("image_url") or all_c[0].get("thumb") or "").strip()
        )
    return (our_img, comp_img)


def our_product_url_from_row(row) -> str:
    """رابط صفحة منتجنا — بعد تطبيع أسماء الأعمدة (رابط_منتجنا / رابط_المنتج / …)."""
    if row is None:
        return ""
    d = row.to_dict() if hasattr(row, "to_dict") else dict(row)
    normalize_result_media_keys(d)
    u = _strip_media_val(d.get("رابط_منتجنا"))
    if not u.startswith("http"):
        return ""
    return u.split()[0]


def competitor_product_url_from_row(row) -> str:
    """رابط صفحة المنتج عند المنافس — أعمدة النتيجة أو جميع_المنافسين أو أسماء مثل abs-size href."""
    if row is None:
        return ""
    d = row.to_dict() if hasattr(row, "to_dict") else dict(row)
    for k in ("رابط_المنافس", "رابط المنافس", "competitor_url"):
        v = _strip_media_val(d.get(k))
        if v.startswith("http"):
            return v.split()[0]
    comp_name = str(d.get("منتج_المنافس", "—"))
    all_c = d.get("جميع_المنافسين", d.get("جميع المنافسين", [])) or []
    if isinstance(all_c, str):
        try:
            all_c = json.loads(all_c)
        except Exception:
            all_c = []
    if isinstance(all_c, list):
        for c in all_c:
            if str(c.get("name", "")).strip() == str(comp_name).strip():
                u = str(c.get("product_url") or c.get("url") or "").strip()
                if u.startswith("http"):
                    return u.split()[0]
        if all_c:
            u = str(all_c[0].get("product_url") or all_c[0].get("url") or "").strip()
            if u.startswith("http"):
                return u.split()[0]
    for k, v in d.items():
        sk = str(k).lower()
        if k in ("رابط_منتجنا", "رابط منتجنا") or "منتجنا" in sk:
            continue
        if "صورة" in str(k) and "وصف" not in str(k) and "href" not in sk:
            continue
        if any(x in sk for x in ("href", "رابط", "link", "url")):
            s = _strip_media_val(v)
            if s.startswith("http"):
                return s.split()[0]
    # أحياناً يُخزَّن رابط صفحة المنتج بالخطأ في عمود الاسم (مثل تصدير المنافس)
    vnm = _strip_media_val(d.get("منتج_المنافس"))
    if vnm.startswith("http"):
        return vnm.split()[0]
    return ""


def safe_results_for_json(results_list):
    """تحويل النتائج لصيغة آمنة للحفظ في JSON/SQLite — يحول القوائم المتداخلة."""
    safe = []
    for r in results_list:
        row = {}
        for k, v in (r.items() if isinstance(r, dict) else {}):
            if isinstance(v, list):
                try:
                    row[k] = json.dumps(v, ensure_ascii=False, default=str)
                except Exception:
                    row[k] = str(v)
            else:
                try:
                    if v is not None and not isinstance(v, (list, dict)) and pd.isna(v):
                        row[k] = "" if k in _MEDIA_KEYS_EMPTY_ON_NA else 0
                        continue
                except (TypeError, ValueError):
                    pass
                row[k] = v
        safe.append(row)
    return safe


def restore_results_from_json(results_list):
    """استعادة النتائج من JSON — يحول نصوص القوائم لقوائم فعلية."""
    restored = []
    for r in results_list:
        row = dict(r) if isinstance(r, dict) else {}
        for k in ["جميع_المنافسين", "جميع المنافسين"]:
            v = row.get(k)
            if isinstance(v, str):
                try:
                    row[k] = json.loads(v)
                except Exception:
                    row[k] = []
            elif v is None:
                row[k] = []
        normalize_result_media_keys(row)
        restored.append(row)
    return restored


def ts_badge(ts_str=""):
    """شارة تاريخ مصغرة (HTML)."""
    if not ts_str:
        ts_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    return (
        f'<span style="font-size:.65rem;color:#555;background:#1a1a2e;'
        f'padding:1px 6px;border-radius:8px;margin-right:4px">🕐 {ts_str}</span>'
    )


def decision_badge(action):
    """شارة قرار معلّق (HTML)."""
    colors = {
        "approved": ("#00C853", "✅ موافق"),
        "deferred": ("#FFD600", "⏸️ مؤجل"),
        "removed": ("#FF1744", "🗑️ محذوف"),
    }
    c, label = colors.get(action, ("#666", action))
    return f'<span style="font-size:.7rem;color:{c};font-weight:700">{label}</span>'


def pid_from_row(row, col):
    """استخراج معرف المنتج من صف pandas بشكل آمن."""
    if not col or col not in row.index:
        return ""
    v = row.get(col, "")
    if v is None or str(v) in ("nan", "None", "", "NaN"):
        return ""
    try:
        fv = float(v)
        if fv == int(fv):
            return str(int(fv))
    except (ValueError, TypeError):
        pass
    return str(v).strip()


def format_missing_for_salla(missing_df: pd.DataFrame) -> pd.DataFrame:
    """تحويل المنتجات المفقودة إلى قالب سلة الشامل المعتمد.

    يُصدّر فقط المنتجات ذات حالة "✅ مفقود مؤكد" لمنع تكرار المنتجات في سلة.
    ملف الـ Raw Data يحتوي على الكل (بما فيها المكرر المحتمل) للمراجعة.
    """
    if missing_df is None or missing_df.empty:
        return pd.DataFrame()

    # فلترة: صادَر لسلة فقط المنتجات المؤكدة (تستثني ⚠️ مكرر محتمل)
    if "حالة_المنتج" in missing_df.columns:
        salla_input = missing_df[
            missing_df["حالة_المنتج"].str.startswith("✅", na=False)
        ].copy()
        if salla_input.empty:
            return pd.DataFrame()
    else:
        salla_input = missing_df  # الملف القديم بدون العمود → يُصدَّر كله

    salla_input = salla_input.reset_index(drop=True)
    n = len(salla_input)
    salla_df = pd.DataFrame(index=salla_input.index)

    def _series_or_blank(col_name: str):
        if col_name in salla_input.columns:
            return salla_input[col_name].fillna("").astype(str)
        return pd.Series([""] * n, index=salla_input.index)

    # 1) بيانات أساسية إجبارية (بعناوين سلة الحرفية)
    salla_df["النوع "] = ["منتج"] * n
    salla_df["أسم المنتج"] = _series_or_blank("منتج_المنافس")
    # تصنيف: يُفضَّل العمود المُطابَق "تصنيف_سلة_الدقيق" ثم فارغ (لا افتراضي أعمى)
    if "تصنيف_سلة_الدقيق" in salla_input.columns:
        salla_df["تصنيف المنتج"] = _series_or_blank("تصنيف_سلة_الدقيق")
    elif "القسم" in salla_input.columns:
        salla_df["تصنيف المنتج"] = _series_or_blank("القسم")
    elif "التصنيف" in salla_input.columns:
        salla_df["تصنيف المنتج"] = _series_or_blank("التصنيف")
    else:
        salla_df["تصنيف المنتج"] = ""
    salla_df["صورة المنتج"] = _series_or_blank("صورة_المنافس")
    salla_df["وصف صورة المنتج"] = ""
    salla_df["نوع المنتج"] = ["منتج جاهز"] * n
    salla_df["سعر المنتج"] = _series_or_blank("سعر_المنافس")
    salla_df["الكمية المتوفرة"] = [0] * n
    # الوصف: يُفضَّل الوصف الآلي (HTML من AI)؛ يُعاد لفارغ إذا لم يُولَّد بعد
    salla_df["الوصف"] = _series_or_blank("الوصف_الآلي")
    salla_df["هل يتطلب شحن؟"] = ["نعم"] * n
    salla_df["رمز المنتج sku"] = _series_or_blank("معرف_المنافس")

    # 2) أعمدة مالية/إدارية
    salla_df["سعر التكلفة"] = ""
    salla_df["السعر المخفض"] = ""
    salla_df["تاريخ بداية التخفيض"] = ""
    salla_df["تاريخ نهاية التخفيض"] = ""
    salla_df["اقصي كمية لكل عميل"] = ""
    salla_df["إخفاء خيار تحديد الكمية"] = ""
    salla_df["اضافة صورة عند الطلب"] = ""
    salla_df["الوزن"] = ""
    salla_df["وحدة الوزن"] = ""

    # 3) الماركة/الحالة — يُفضَّل العمود المُطابَق "الماركة_المعتمدة" (نسخ حرفي من سلة)
    salla_df["حالة المنتج"] = ""
    salla_df["الماركة"] = (
        _series_or_blank("الماركة_المعتمدة")
        if "الماركة_المعتمدة" in salla_input.columns
        else _series_or_blank("الماركة")
    )

    # 4) بقية الأعمدة القياسية
    salla_df["العنوان الترويجي"] = ""
    salla_df["تثبيت المنتج"] = ""
    salla_df["الباركود"] = ""
    salla_df["السعرات الحرارية"] = ""
    salla_df["MPN"] = ""
    salla_df["GTIN"] = ""
    salla_df["خاضع للضريبة ؟"] = ["نعم"] * n
    salla_df["سبب عدم الخضوع للضريبة"] = ""

    return salla_df.reset_index(drop=True)


# ══════════════════════════════════════════════════════════════════════════════
#  طبقة مطابقة سلة — Salla Validation Layer
# ══════════════════════════════════════════════════════════════════════════════

def map_salla_categories(
    missing_df: pd.DataFrame,
    categories_csv_path: str = "",
) -> pd.DataFrame:
    """
    يقرأ ملف التصنيفات المعتمد ويعيّن مسار التصنيف الدقيق في سلة
    بناءً على عمودَي (النوع) و (الجنس) في كل صف.

    يُضيف عمود "تصنيف_سلة_الدقيق" إلى DataFrame.
    """
    if missing_df is None or missing_df.empty:
        return missing_df

    # ── تحديد مسار ملف التصنيفات ─────────────────────────────────────────────
    if not categories_csv_path:
        from utils.data_paths import get_catalog_data_path
        categories_csv_path = get_catalog_data_path("تصنيفات مهووس.csv")
        if not __import__("os").path.exists(categories_csv_path):
            categories_csv_path = get_catalog_data_path("categories.csv")

    valid_categories: list = []
    try:
        cat_df  = pd.read_csv(categories_csv_path, encoding="utf-8-sig")
        cat_col = "التصنيفات" if "التصنيفات" in cat_df.columns else cat_df.columns[0]
        valid_categories = cat_df[cat_col].dropna().astype(str).tolist()
    except Exception:
        pass

    # خريطة بحث ثابتة: (gender, type) → كلمة بحث بالعربية
    _SEARCH_MAP = {
        ("رجالي",   "hair_mist"):  "عطور شعر",
        ("نسائي",   "hair_mist"):  "عطور شعر",
        ("للجنسين", "hair_mist"):  "عطور شعر",
        ("",        "hair_mist"):  "عطور شعر",
        ("رجالي",   "body_mist"):  "معطرات جسم",
        ("نسائي",   "body_mist"):  "معطرات جسم",
        ("",        "body_mist"):  "معطرات جسم",
        ("رجالي",   ""):           "عطور رجالية",
        ("نسائي",   ""):           "عطور نسائية",
        ("للجنسين", ""):           "عطور للجنسين",
    }

    def _get_best_category(row) -> str:
        if not valid_categories:
            return ""
        gender = str(row.get("الجنس", "") or "").strip()
        type_  = str(row.get("النوع", "") or "").strip().lower()
        search_term = _SEARCH_MAP.get((gender, type_)) \
            or _SEARCH_MAP.get(("", type_)) \
            or _SEARCH_MAP.get((gender, "")) \
            or "عطور"
        hit = rf_proc.extractOne(search_term, valid_categories, scorer=rf_fuzz.token_set_ratio)
        if hit and hit[1] >= 60:
            return hit[0]
        return valid_categories[0] if valid_categories else ""

    out = missing_df.copy()
    out["تصنيف_سلة_الدقيق"] = out.apply(_get_best_category, axis=1)
    return out


def validate_salla_brands(
    missing_df: pd.DataFrame,
    brands_csv_path: str = "",
) -> tuple:
    """
    يطابق ماركة كل منتج مع قائمة الماركات المعتمدة في سلة.

    يُضيف عمود "الماركة_المعتمدة":
      - إذا وُجد تطابق ≥ 85%: الاسم الحرفي من ملف سلة (نسخ حرفي).
      - إذا لم يوجد تطابق: الاسم الأصلي يُبقى كما هو.

    يُرجع: (DataFrame_المُحدَّث, قائمة_الماركات_المفقودة)
    """
    if missing_df is None or missing_df.empty:
        return missing_df, []

    # ── تحديد مسار ملف الماركات ──────────────────────────────────────────────
    if not brands_csv_path:
        from utils.data_paths import get_catalog_data_path
        brands_csv_path = get_catalog_data_path("ماركات مهووس.csv")
        if not __import__("os").path.exists(brands_csv_path):
            brands_csv_path = get_catalog_data_path("brands.csv")

    valid_brands: list = []
    try:
        brands_df   = pd.read_csv(brands_csv_path, encoding="utf-8-sig")
        brand_col   = "اسم الماركة" if "اسم الماركة" in brands_df.columns else brands_df.columns[0]
        valid_brands = brands_df[brand_col].dropna().astype(str).tolist()
    except Exception:
        pass

    missing_brands: set = set()

    def _get_valid_brand(brand_name) -> str:
        bname = str(brand_name or "").strip()
        if not bname or bname.lower() in ("nan", "none", ""):
            return ""
        if not valid_brands:
            return bname
        hit = rf_proc.extractOne(bname, valid_brands, scorer=rf_fuzz.token_set_ratio)
        if hit and hit[1] >= 85:
            return hit[0]  # الاسم الحرفي من ملف سلة — نسخ حرفي
        # ماركة غير مسجلة → سجّلها لتنبيه المستخدم
        missing_brands.add(bname)
        return bname

    out = missing_df.copy()
    brand_src = "الماركة" if "الماركة" in out.columns else None
    out["الماركة_المعتمدة"] = (
        out[brand_src].apply(_get_valid_brand) if brand_src
        else ""
    )
    return out, sorted(missing_brands)


# ══════════════════════════════════════════════════════════════════════════════
#  ذاكرة المنافسين التراكمية — Competitor Master Catalog (Upsert Logic)
# ══════════════════════════════════════════════════════════════════════════════

def upsert_competitors(new_comp_dfs: dict) -> pd.DataFrame:
    """
    يدمج الملفات الجديدة المرفوعة مع الكتالوج المركزي المحفوظ على القرص.

    القواعد:
    - إذا كان المنتج موجوداً (نفس المنافس + نفس الرابط): يُحدَّث السعر وتاريخ الرصد.
    - إذا كان المنتج جديداً: يُضاف.
    - لا يُحذف أي منتج قديم إطلاقاً.

    يُرجع: master_df كـ dict {اسم_المتجر: DataFrame} جاهز للتمرير لـ run_full_analysis.
    """
    from utils.data_paths import get_master_competitors_path

    master_path = get_master_competitors_path()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")

    # ── دمج الملفات الجديدة إن وُجدت ──────────────────────────────────────
    if new_comp_dfs:
        frames = []
        for store_name, df in new_comp_dfs.items():
            df = df.copy()
            # حقن اسم المتجر إذا لم يكن موجوداً
            if "store" not in df.columns and "المنافس" not in df.columns:
                df["store"] = store_name
            df["تاريخ_الرصد"] = now_str
            frames.append(df)
        new_df = pd.concat(frames, ignore_index=True)
    else:
        new_df = pd.DataFrame()

    # ── تحميل الكتالوج القديم ──────────────────────────────────────────────
    if os.path.exists(master_path):
        try:
            master_df = pd.read_csv(master_path, encoding="utf-8-sig", low_memory=False)
        except Exception:
            master_df = pd.DataFrame()
    else:
        master_df = pd.DataFrame()

    # ── دمج القديم + الجديد ────────────────────────────────────────────────
    if new_df.empty and master_df.empty:
        return {}

    if new_df.empty:
        combined = master_df
    elif master_df.empty:
        combined = new_df
    else:
        combined = pd.concat([master_df, new_df], ignore_index=True)

    # ── تطبيع عمود الرصد حتى يُمكن الفرز عليه ───────────────────────────
    if "تاريخ_الرصد" not in combined.columns:
        combined["تاريخ_الرصد"] = now_str

    combined["تاريخ_الرصد"] = combined["تاريخ_الرصد"].fillna(now_str).astype(str)
    combined = combined.sort_values("تاريخ_الرصد", kind="stable")

    # ── إزالة التكرار: الأحدث يربح (keep='last' بعد الفرز) ───────────────
    store_col = next((c for c in ["المنافس", "store", "متجر"] if c in combined.columns), None)
    url_col = next((c for c in ["رابط_المنافس", "رابط المنتج", "url", "link", "رابط"] if c in combined.columns), None)

    if store_col and url_col:
        before = len(combined)
        combined = combined.drop_duplicates(subset=[store_col, url_col], keep="last")
        deduped = before - len(combined)
    else:
        deduped = 0

    combined = combined.reset_index(drop=True)

    # ── حفظ الكتالوج المتراكم ─────────────────────────────────────────────
    try:
        combined.to_csv(master_path, index=False, encoding="utf-8-sig")
    except Exception:
        pass

    # ── إرجاع dict مقسَّم حسب المتجر (لـ run_full_analysis) ──────────────
    result: dict = {}
    split_col = store_col  # قد يكون "store" أو "المنافس" أو None
    if split_col and split_col in combined.columns:
        for sname, sdf in combined.groupby(split_col, sort=False):
            result[str(sname).strip() or "master_competitors"] = sdf.reset_index(drop=True)
    else:
        result["master_competitors.csv"] = combined

    return result, len(combined), deduped
