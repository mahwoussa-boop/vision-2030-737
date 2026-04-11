"""
تصدير منتجات مفقودة بتنسيق CSV سلة الشامل المحدث.
مطابق تماماً لنموذج "منتججديد.csv" المرفق.
v29.1 - وضع المطابقة الصارمة (Strict Matching) لتجنب أخطاء الاستيراد.

✅ إصلاح #1: UnboundLocalError في resolve_category_for_shamel (r غير مُعرَّف)
✅ إصلاح #2: Double BOM — إزالة \\ufeff اليدوي + استخدام encode("utf-8-sig")
✅ إصلاح #3: مسافة زائدة في عمود "النوع " أصبحت "النوع"
"""
import html
import io
import csv
import re
import json
import uuid
import difflib
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional

import pandas as pd

from engines.ai_engine import auto_infer_category, generate_mahwous_description
from engines.mahwous_core import sanitize_salla_text, format_mahwous_description
from engines.prompts import (
    SALLA_BRANDS_FILE, SALLA_BRANDS_COL,
    BRANDS_CSV_FILE, BRANDS_CSV_COL,
    SALLA_CATEGORIES_FILE, SALLA_CATEGORIES_COL,
    CATEGORIES_CSV_FILE, CATEGORIES_CSV_COL,
)
from utils.data_paths import get_catalog_data_path
from utils.helpers import safe_float
from utils.data_sanitizer import (
    standardize_terms,
    sanitize_description_terms,
    get_brand_arabic_name,
    validate_product_data,
    extract_size_ml,
)

_HTML_TAG_RE = re.compile(r"<[^>]+>")
_ALT_SAFE_RE = re.compile(r"[^0-9A-Za-z\u0600-\u06FF\s]")


def _markdown_to_salla_html(md_text: str) -> str:
    """تحويل نص Markdown إلى HTML بسيط مناسب لسلة."""
    lines = md_text.strip().split("\n")
    out = []
    in_ul = False
    for line in lines:
        line = line.rstrip()
        if line.startswith("### "):
            if in_ul: out.append("</ul>"); in_ul = False
            out.append(f"<h3>{line[4:].strip()}</h3>")
        elif line.startswith("## "):
            if in_ul: out.append("</ul>"); in_ul = False
            out.append(f"<h2>{line[3:].strip()}</h2>")
        elif line.startswith("# "):
            if in_ul: out.append("</ul>"); in_ul = False
            out.append(f"<h2>{line[2:].strip()}</h2>")
        elif line.startswith("* ") or line.startswith("- "):
            if not in_ul: out.append("<ul>"); in_ul = True
            item = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", line[2:].strip())
            out.append(f"<li>{item}</li>")
        elif not line.strip():
            if in_ul: out.append("</ul>"); in_ul = False
        else:
            if in_ul: out.append("</ul>"); in_ul = False
            para = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", line)
            out.append(f"<p>{para}</p>")
    if in_ul: out.append("</ul>")
    return "\n".join(out)


def _debug_log(hypothesis_id: str, location: str, message: str, data: dict | None = None, run_id: str = "pre-fix") -> None:
    pass


def _norm_brand(s: str) -> str:
    t = sanitize_salla_text(str(s or "")).strip().lower()
    t = re.sub(r"[\|\-_/]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _load_store_brands() -> list[str]:
    """تحميل الماركات الرسمية — يجرب عدة أسماء ملفات (مسافة / شرطة سفلية)."""
    brands: list[str] = []
    # الأولوية: ماركات_مهووس.csv (underscore) ثم ماركات مهووس.csv (space) ثم brands.csv
    candidate_files = [
        (SALLA_BRANDS_FILE.replace(" ", "_"), SALLA_BRANDS_COL),  # ماركات_مهووس.csv
        (SALLA_BRANDS_FILE, SALLA_BRANDS_COL),                    # ماركات مهووس.csv
        (BRANDS_CSV_FILE,   None),                                  # brands.csv
    ]
    for fname, col_name in candidate_files:
        try:
            path = get_catalog_data_path(fname)
            if path and pd.io.common.file_exists(path):
                _df = pd.read_csv(path, encoding="utf-8-sig")
                if col_name:
                    if col_name in _df.columns:
                        brands = [str(x).strip() for x in _df[col_name].dropna().astype(str).tolist() if str(x).strip()]
                else:
                    col = "اسم الماركة" if "اسم الماركة" in _df.columns else ("الاسم" if "الاسم" in _df.columns else _df.columns[0])
                    brands = [str(x).strip() for x in _df[col].dropna().astype(str).tolist() if str(x).strip()]
                if brands:
                    return list(dict.fromkeys(brands))
        except Exception:
            continue
    return []


def _load_store_categories() -> list[str]:
    """تحميل التصنيفات — يجرب عدة أسماء ملفات (مسافة / شرطة سفلية)."""
    cats: list[str] = []
    candidate_files = [
        (SALLA_CATEGORIES_FILE.replace(" ", "_"), SALLA_CATEGORIES_COL),  # تصنيفات_مهووس.csv
        (SALLA_CATEGORIES_FILE, SALLA_CATEGORIES_COL),                    # تصنيفات مهووس.csv
        (CATEGORIES_CSV_FILE,   None),                                     # categories.csv
    ]
    for fname, col_name in candidate_files:
        try:
            path = get_catalog_data_path(fname)
            if path and pd.io.common.file_exists(path):
                _df = pd.read_csv(path, encoding="utf-8-sig")
                if col_name:
                    if col_name in _df.columns:
                        cats = [str(x).strip() for x in _df[col_name].dropna().astype(str).tolist() if str(x).strip()]
                else:
                    col = "التصنيفات" if "التصنيفات" in _df.columns else ("الاسم" if "الاسم" in _df.columns else _df.columns[0])
                    cats = [str(x).strip() for x in _df[col].dropna().astype(str).tolist() if str(x).strip()]
                if cats:
                    return list(dict.fromkeys(cats))
        except Exception:
            continue
    return []


def _brand_aliases(brand_label: str) -> set[str]:
    """يبني مفاتيح مطابقة لاسم ماركة مركب مثل: عربي | English."""
    s = str(brand_label or "").strip()
    if not s:
        return set()
    parts = [p.strip() for p in re.split(r"[|/\\\-]+", s) if p.strip()]
    keys = {_norm_brand(s)}
    keys.update({_norm_brand(p) for p in parts})
    return {k for k in keys if k}


def _resolve_brand_to_store(brand_value: str, store_brands: list[str]) -> str:
    """
    (مطابقة صارمة): يرجع الاسم الرسمي المطابق من الكتالوج.
    يدعم تنسيق عربي | English في ملف الماركات.
    إذا لم يجد الماركة في ملفاتك، يرجع فارغاً لتجنب خطأ سلة.
    """
    import re as _re
    bv = str(brand_value or "").strip()
    if not bv or not store_brands:
        return ""
    if bv in store_brands:
        return bv
    target_keys = _brand_aliases(bv)
    if not target_keys:
        return ""
    for sb in store_brands:
        if target_keys & _brand_aliases(sb):
            return sb
    # مطابقة جزئية: هل قيمة الماركة موجودة كجزء داخل ماركة الكتالوج؟
    bv_lower = bv.lower().strip()
    for sb in store_brands:
        parts = [p.strip().lower() for p in _re.split(r"[|/\\]", sb.lower()) if p.strip()]
        if bv_lower in parts or any(bv_lower == p or bv_lower in p.split() for p in parts):
            return sb
    return ""  # إرجاع فارغ لمنع الرفض


def _norm_category(s: str) -> str:
    t = sanitize_salla_text(str(s or "")).strip().lower()
    t = re.sub(r"[>|\-_/]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _resolve_category_to_store(cat_value: str, store_categories: list[str], gender_hint: str = "") -> str:
    """
    (مطابقة صارمة): يرجع التصنيف المطابق.
    إذا لم يجد التصنيف في ملفاتك، يرجع فارغاً لتجنب خطأ سلة.
    """
    cv = str(cat_value or "").strip()
    if not store_categories:
        return ""

    if cv and cv in store_categories:
        return cv

    norm_map = {_norm_category(c): c for c in store_categories if _norm_category(c)}
    ncv = _norm_category(cv)
    if ncv and ncv in norm_map:
        return norm_map[ncv]

    gh = str(gender_hint or "").strip()
    # المطابقة تعمل مع "للنساء" و"نسائي" كلاهما
    if gh in ("للنساء", "نسائي"):
        for c in store_categories:
            if "نسائي" in c:
                return c
    elif gh in ("للرجال", "رجالي"):
        for c in store_categories:
            if "رجالي" in c:
                return c
    elif gh == "للجنسين":
        for c in store_categories:
            if "للجنسين" in c:
                return c

    if ncv:
        matches = difflib.get_close_matches(ncv, list(norm_map.keys()), n=1, cutoff=0.5)
        if matches:
            return norm_map[matches[0]]

    return ""  # إرجاع فارغ لمنع الرفض


def resolve_brand_for_shamel(brand_raw: str) -> str:
    """
    يطابق اسم الماركة القادم من الـ AI أو الكشط مع الاسم الرسمي في
    «ماركات مهووس.csv» أو «brands.csv» (نفس منطق التصدير).
    يُرجع سلسلة فارغة إن لم يُعثر على ملف أو على مطابقة.
    """
    bv = sanitize_salla_text(str(brand_raw or "").strip())
    if not bv or bv.lower() in ("ماركة عالمية", "unknown", "nan"):
        return ""
    store = _load_store_brands()
    if not store:
        return ""
    brand = get_brand_arabic_name(bv, store) if bv else ""
    if not brand and bv:
        brand = _resolve_brand_to_store(bv, store)
    return brand or ""


def resolve_category_for_shamel(
    category_raw: str,
    gender_hint: str = "",
    product_name_fallback: str = "",
) -> str:
    """
    يطابق التصنيف مع «تصنيفات مهووس.csv» / categories.csv (نفس منطق التصدير).
    يدعم صيغة الـ AI «العطور > عطور رجالية» بمحاولة الجزء بعد > عند فشل المطابقة الكاملة.

    ✅ إصلاح #1: تهيئة r = "" قبل الحلقة لمنع UnboundLocalError عند _cands فارغة
    أو عند استدعاء الدالة بـ gender_hint="للجنسين" وقائمة تصنيفات فارغة.
    """
    store = _load_store_categories()
    if not store:
        return ""

    gh = str(gender_hint or "").strip()

    cv = sanitize_salla_text(str(category_raw or "").strip())
    if not cv and product_name_fallback:
        cv = sanitize_salla_text(auto_infer_category(product_name_fallback, gh))

    def _try(x: str) -> str:
        x = sanitize_salla_text(str(x or "").strip())
        if not x:
            return ""
        return _resolve_category_to_store(x, store, gh) or ""

    # الجزء الأخصّ أولاً: «العطور > عطور رجالية» → جرّب «عطور رجالية» ثم «العطور» ثم النص كاملاً
    _cands: list[str] = []
    if ">" in cv:
        parts = [
            sanitize_salla_text(p.strip())
            for p in cv.split(">")
            if sanitize_salla_text(p.strip())
        ]
        for p in reversed(parts):
            _cands.append(p)
        _cands.append(cv)
    else:
        _cands.append(cv)

    # ✅ إصلاح #1: تهيئة r قبل الحلقة — يمنع UnboundLocalError إذا كانت _cands فارغة
    r: str = ""
    for cand in _cands:
        r = _try(cand)
        if r:
            return r

    # r مُعرَّف دائماً هنا (سلسلة فارغة في أسوأ الأحوال)
    if gh == "للجنسين" and not r:
        for c in store:
            if c.strip() == "عطور للجنسين":
                return c
        if "العطور" in store:
            return "العطور"

    return ""


def _concentration_ar(s: str) -> str:
    """استخراج التركيز بالصيغة الموحّدة المعتمدة لمتجر مهووس."""
    t = str(s or "").lower()
    if any(x in t for x in ("extrait",)):
        return "إكسترا دو بارفيم"
    if any(x in t for x in ("eau de parfum", "edp", "أو دو بارفان", "بارفيوم")):
        return "أو دو بارفيوم"
    if any(x in t for x in ("eau de toilette", "edt")):
        return "أو دو تواليت"
    if any(x in t for x in ("eau de cologne", "edc")):
        return "أو دو كولون"
    if "parfum" in t:
        return "بارفيم"
    return ""


def _brand_display(brand: str) -> str:
    """للعرض في العنوان: يأخذ الجزء العربي إذا كان التنسيق 'عربي | English'."""
    if not brand:
        return ""
    if "|" in brand:
        parts = [p.strip() for p in brand.split("|")]
        # إذا وُجد جزء عربي، أعده؛ وإلا الأول
        ar_parts = [p for p in parts if re.search(r"[؀-ۿ]", p)]
        if ar_parts:
            return ar_parts[0]
        return parts[0]
    return brand.strip()


def _build_export_title(raw_name: str, brand: str, gender: str) -> str:
    """عنوان يبدأ بكلمة عطر مع الاحتفاظ بالاسم الإنجليزي إذا لم يوجد عربي."""
    _sz = safe_float(re.search(r"(\d{2,4})\s*ml", str(raw_name).lower()).group(1), 0) if re.search(r"(\d{2,4})\s*ml", str(raw_name).lower()) else 0
    size_txt = f"{int(_sz)} مل" if _sz > 0 else ""
    conc_txt = _concentration_ar(raw_name)
    line_txt = sanitize_salla_text(str(raw_name or "").strip())

    # إزالة جميع مرادفات الماركة من اسم المنتج لتجنب التكرار
    line_norm = _norm_brand(line_txt)
    for _bk in sorted(_brand_aliases(brand), key=len, reverse=True):
        if _bk:
            line_norm = re.sub(rf"\b{re.escape(_bk)}\b", " ", line_norm, flags=re.I)
    line_norm = re.sub(r"\s+", " ", line_norm).strip()

    # محاولة استخراج النص العربي أولاً
    line_ar = " ".join(re.findall(r"[\u0600-\u06FF]+", line_norm))
    line_ar = sanitize_salla_text(line_ar).strip()

    # إذا لم يوجد نص عربي → استخدم الاسم المنظّف (إنجليزي أو مختلط)
    if not line_ar:
        clean_name = re.sub(r"\b(edp|edt|edc|parfum|cologne|extrait)\b", "", line_txt, flags=re.I)
        clean_name = re.sub(r"\b\d{2,4}\s*ml\b", "", clean_name, flags=re.I)
        # احذف جميع مرادفات الماركة من البداية
        for alias in sorted(_brand_aliases(brand), key=len, reverse=True):
            if alias and clean_name.lower().startswith(alias):
                clean_name = clean_name[len(alias):].strip()
                break
        line_ar = re.sub(r"\s+", " ", clean_name).strip()
        if not line_ar:
            line_ar = line_txt

    brand_disp = _brand_display(brand)
    pieces = ["عطر", brand_disp, line_ar, conc_txt, size_txt, gender]
    title = " ".join([p for p in pieces if p]).strip()
    title = re.sub(r"\s+", " ", title)
    return title[:220]


# ✅ إصلاح #3: إزالة المسافة الزائدة من "النوع " → "النوع"
# القائمة مطابقة لقالب سلة الشامل الرسمي بدون مسافات زائدة
SALLA_SHAMEL_COLUMNS = [
    "النوع",
    "أسم المنتج",
    "تصنيف المنتج",
    "صورة المنتج",
    "وصف صورة المنتج",
    "نوع المنتج",
    "سعر المنتج",
    "الوصف",
    "هل يتطلب شحن؟",
    "رمز المنتج sku",
    "سعر التكلفة",
    "السعر المخفض",
    "تاريخ بداية التخفيض",
    "تاريخ نهاية التخفيض",
    "اقصي كمية لكل عميل",
    "إخفاء خيار تحديد الكمية",
    "اضافة صورة عند الطلب",
    "الوزن",
    "وحدة الوزن",
    "الماركة",
    "العنوان الترويجي",
    "تثبيت المنتج",
    "الباركود",
    "السعرات الحرارية",
    "MPN",
    "GTIN",
    "خاضع للضريبة ؟",
    "سبب عدم الخضوع للضريبة",
    "[1] الاسم",
    "[1] النوع",
    "[1] القيمة",
    "[1] الصورة / اللون",
    "[2] الاسم",
    "[2] النوع",
    "[2] القيمة",
    "[2] الصورة / اللون",
    "[3] الاسم",
    "[3] النوع",
    "[3] القيمة",
    "[3] الصورة / اللون"
]


def _strip_html_visible(s: str) -> str:
    if not s:
        return ""
    t = _HTML_TAG_RE.sub(" ", str(s))
    t = html.unescape(t)
    return re.sub(r"\s+", " ", t).strip()


def _is_url_text(s: str) -> bool:
    t = str(s or "").strip().lower()
    return t.startswith("http://") or t.startswith("https://")


def _plain_missing_product_name(r: dict) -> str:
    for key in ("المنتج", "اسم المنتج", "اسم_المنتج", "Product", "Name", "name", "title", "الاسم", "منتج_المنافس", "أسم المنتج"):
        v = r.get(key)
        if v and not _is_url_text(v):
            return sanitize_salla_text(str(v))
    return "منتج عطر"


def _contains_arabic(s: str) -> bool:
    return bool(re.search(r"[\u0600-\u06FF]", str(s or "")))


def _infer_gender_text(r: dict) -> str:
    raw = " ".join([
        str(r.get("الجنس", "")),
        str(r.get("نوع المنتج", "")),
        str(r.get("منتج_المنافس", "")),
        str(r.get("المنتج", "")),
        str(r.get("أسم المنتج", "")),
    ]).lower()
    if any(x in raw for x in ("نسائي", "نساء", "للنساء", "women", "female", "lady", "pour femme")):
        return "للنساء"
    if any(x in raw for x in ("رجالي", "رجال", "للرجال", "men", "male", "homme", "pour homme")):
        return "للرجال"
    if any(x in raw for x in ("للجنسين", "unisex", "الجنسين")):
        return "للجنسين"
    return ""


def _safe_alt_text(s: str) -> str:
    t = sanitize_salla_text(_strip_html_visible(s or "")).strip()
    t = _ALT_SAFE_RE.sub(" ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t[:180]


def _real_price(r: dict) -> str:
    for k in ("سعر_المنافس", "سعر المنافس", "السعر", "سعر المنتج", "Price", "price", "PRICE"):
        if k not in r: continue
        p = safe_float(r.get(k), 0.0)
        if p > 0: return str(round(p, 2))
    return ""


def _real_sku(r: dict) -> str:
    for k in ("معرف_المنافس", "رمز المنتج sku", "رمز_المنتج_sku", "SKU", "sku", "Sku", "رمز المنتج", "رمز_المنتج", "رقم المنتج", "Barcode", "barcode", "الباركود", "الكود", "كود", "Code", "code"):
        v = r.get(k)
        if v is None or (isinstance(v, float) and pd.isna(v)): continue
        s = str(v).strip()
        if not s or s.lower() in ("nan", "none", "<na>") or s.startswith("http"): continue
        return s
    return ""


def export_to_salla_shamel(missing_df: pd.DataFrame, generate_descriptions: bool = True) -> bytes:
    """
    تصدير DataFrame المنتجات المفقودة إلى ملف CSV بتنسيق سلة الشامل.

    ✅ إصلاح #2: إزالة BOM اليدوي ("\\ufeff") واستخدام encode("utf-8-sig") مباشرة.
    encode("utf-8-sig") تضيف BOM واحداً صحيحاً (EF BB BF) في بداية الملف تلقائياً،
    بينما الجمع بين "\\ufeff" + encode("utf-8") ينتج BOM مزدوجاً يكسر الاستيراد.

    ✅ إصلاح #3: مفتاح "النوع" بدون مسافة زائدة في row_csv.
    """
    ncols = len(SALLA_SHAMEL_COLUMNS)

    if missing_df is None or missing_df.empty:
        out = io.StringIO(newline="")
        w = csv.writer(out)
        # ✅ إصلاح #2: بدون "\ufeff" يدوي في النص — encode("utf-8-sig") ستضيفه تلقائياً
        w.writerow(["بيانات المنتج"] + [""] * (ncols - 1))
        w.writerow(SALLA_SHAMEL_COLUMNS)
        # ✅ إصلاح #2: encode("utf-8-sig") بدلاً من ("\ufeff" + ...).encode("utf-8")
        return out.getvalue().encode("utf-8-sig")

    rows_out = []
    _store_brands = _load_store_brands()
    _store_categories = _load_store_categories()

    _seen_skus: set[str] = set()
    for _, row in missing_df.iterrows():
        r = row.to_dict()
        raw_pname = _plain_missing_product_name(r)
        gender_inferred = _infer_gender_text(r)
        comp_price = _real_price(r)

        # 1. الماركة - مطابقة صارمة
        brand_raw = sanitize_salla_text(
            str(r.get("الماركة_الرسمية", "") or r.get("الماركة", "")).strip()
        )
        if brand_raw in ("", "ماركة عالمية", "Unknown", "unknown"):
            brand_raw = ""
        # مطابقة مرنة أولاً ثم المطابق الصارم احتياطاً
        brand = get_brand_arabic_name(brand_raw, _store_brands) if brand_raw else ""
        if not brand and brand_raw:
            brand = _resolve_brand_to_store(brand_raw, _store_brands)

        pname = _build_export_title(raw_pname, brand, gender_inferred)

        # فحص الحجم المفقود — علامة ⚠️ للمراجعة اليدوية
        _size_check = extract_size_ml(raw_pname)
        if not _size_check and "مل" not in pname:
            pname = "⚠️ " + pname

        # 2. رمز المنتج (SKU) - منع التكرار
        img = str(r.get("صورة_المنافس", r.get("image_url", ""))).strip()
        sku_raw = _real_sku(r)
        sku = sanitize_salla_text(str(sku_raw or "").strip())
        _sku_invalid = (not sku) or ("/" in sku) or ("http" in sku.lower()) or (sku in _seen_skus)
        if _sku_invalid:
            sku = f"MS-{uuid.uuid4().hex[:10].upper()}"
        _seen_skus.add(sku)

        # 3. الوصف
        product_data = {
            "name": pname,
            "brand": brand,
            "description": str(r.get("الوصف", "")),
            "notes": {
                "top": str(r.get("الافتتاحية", r.get("top_notes", ""))),
                "heart": str(r.get("القلب", r.get("heart_notes", ""))),
                "base": str(r.get("القاعدة", r.get("base_notes", "")))
            }
        }
        desc_text = format_mahwous_description(product_data)

        # 4. التصنيف - مطابقة صارمة
        category_raw = sanitize_salla_text(
            str(r.get("التصنيف_الرسمي", "") or r.get("تصنيف المنتج", "")).strip()
        )
        if not category_raw:
            category_raw = auto_infer_category(pname, str(r.get("الجنس", "")))

        category = _resolve_category_to_store(category_raw, _store_categories, gender_inferred)

        alt_txt = _safe_alt_text(f"صورة {pname}")

        # ── الوصف: لا نستخدم sanitize_salla_text لأنها تحذف وسوم HTML ──
        raw_ai_desc = str(r.get("وصف_AI", "") or "").strip()
        if raw_ai_desc:
            # إذا كان الوصف بصيغة Markdown (يحتوي ## أو ###) حوّله إلى HTML
            if "##" in raw_ai_desc or "\n### " in raw_ai_desc or "\n## " in raw_ai_desc:
                final_desc = _markdown_to_salla_html(raw_ai_desc)
            else:
                # وصف HTML جاهز: نظّفه بدون حذف وسوم HTML
                final_desc = raw_ai_desc.strip()
            final_desc = sanitize_description_terms(final_desc)
        else:
            final_desc = desc_text  # fallback: HTML من format_mahwous_description

        promo = sanitize_salla_text(str(r.get("العنوان الترويجي", "") or "").strip())

        barcode_out = str(r.get("الباركود", "") or "").strip()
        if not barcode_out:
            barcode_out = str(r.get("barcode", "") or "").strip()
        gtin_out = barcode_out if barcode_out.isdigit() and len(barcode_out) >= 8 else ""

        # ✅ إصلاح #3: المفتاح "النوع" بدون مسافة زائدة ليطابق SALLA_SHAMEL_COLUMNS
        row_csv = {
            "النوع": "منتج",
            "أسم المنتج": pname,
            "تصنيف المنتج": category,
            "صورة المنتج": img,
            "وصف صورة المنتج": alt_txt,
            "نوع المنتج": "منتج جاهز",
            "سعر المنتج": comp_price,
            "الوصف": final_desc,
            "هل يتطلب شحن؟": "نعم",
            "رمز المنتج sku": sku,
            "سعر التكلفة": "",
            "السعر المخفض": "",
            "تاريخ بداية التخفيض": "",
            "تاريخ نهاية التخفيض": "",
            "اقصي كمية لكل عميل": "1",
            "إخفاء خيار تحديد الكمية": "لا",
            "اضافة صورة عند الطلب": "لا",
            "الوزن": "0.2",
            "وحدة الوزن": "kg",
            "الماركة": brand,
            "العنوان الترويجي": promo,
            "تثبيت المنتج": "",
            "الباركود": barcode_out,
            "السعرات الحرارية": "",
            "MPN": "",
            "GTIN": gtin_out,
            "خاضع للضريبة ؟": "نعم",
            "سبب عدم الخضوع للضريبة": "",
            "[1] الاسم": "", "[1] النوع": "", "[1] القيمة": "", "[1] الصورة / اللون": "",
            "[2] الاسم": "", "[2] النوع": "", "[2] القيمة": "", "[2] الصورة / اللون": "",
            "[3] الاسم": "", "[3] النوع": "", "[3] القيمة": "", "[3] الصورة / اللون": "",
        }
        rows_out.append([row_csv.get(c, "") for c in SALLA_SHAMEL_COLUMNS])

    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    # ✅ إصلاح #2: بدون "\ufeff" يدوي — encode("utf-8-sig") ستضيف BOM صحيحاً تلقائياً
    writer.writerow(["بيانات المنتج"] + [""] * (ncols - 1))
    writer.writerow(SALLA_SHAMEL_COLUMNS)
    for line in rows_out:
        writer.writerow(line)

    # ✅ إصلاح #2: encode("utf-8-sig") بدلاً من ("\ufeff" + buf.getvalue()).encode("utf-8")
    return buf.getvalue().encode("utf-8-sig")
