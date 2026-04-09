"""
تصدير منتجات مفقودة بتنسيق CSV سلة الشامل المحدث.
مطابق تماماً لنموذج "منتججديد.csv" المرفق.
v28.0 - النسخة الكاملة المدمجة.
"""
import html
import io
import csv
import re
import json
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional

import pandas as pd

from engines.ai_engine import auto_infer_category, generate_mahwous_description
from engines.mahwous_core import sanitize_salla_text, format_mahwous_description
from engines.prompts import (
    SALLA_BRANDS_FILE, SALLA_BRANDS_COL,
    BRANDS_CSV_FILE, BRANDS_CSV_COL,
)
from utils.data_paths import get_catalog_data_path
from utils.helpers import safe_float

_HTML_TAG_RE = re.compile(r"<[^>]+>")
_ALT_SAFE_RE = re.compile(r"[^0-9A-Za-z\u0600-\u06FF\s]")


def _debug_log(hypothesis_id: str, location: str, message: str, data: dict | None = None, run_id: str = "pre-fix") -> None:
    # region agent log
    try:
        payload = {
            "sessionId": "aea738",
            "runId": run_id,
            "hypothesisId": hypothesis_id,
            "location": location,
            "message": message,
            "data": data or {},
            "timestamp": int(datetime.now().timestamp() * 1000),
        }
        with open("debug-aea738.log", "a", encoding="utf-8") as _fh:
            _fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception:
        pass
    # endregion


def _norm_brand(s: str) -> str:
    t = sanitize_salla_text(str(s or "")).strip().lower()
    t = re.sub(r"[\|\-_/]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _load_store_brands() -> list[str]:
    """تحميل الماركات الرسمية من ملفات الكتالوج كما هي."""
    brands: list[str] = []
    salla_path = get_catalog_data_path(SALLA_BRANDS_FILE)
    fallback_path = get_catalog_data_path(BRANDS_CSV_FILE)
    try:
        if salla_path and pd.io.common.file_exists(salla_path):
            _df = pd.read_csv(salla_path, encoding="utf-8-sig")
            if SALLA_BRANDS_COL in _df.columns:
                brands = [str(x).strip() for x in _df[SALLA_BRANDS_COL].dropna().astype(str).tolist() if str(x).strip()]
    except Exception:
        brands = []
    if brands:
        return list(dict.fromkeys(brands))
    try:
        if fallback_path and pd.io.common.file_exists(fallback_path):
            _df = pd.read_csv(fallback_path, encoding="utf-8-sig", header=None)
            if BRANDS_CSV_COL < len(_df.columns):
                brands = [str(x).strip() for x in _df.iloc[:, BRANDS_CSV_COL].dropna().astype(str).tolist() if str(x).strip()]
    except Exception:
        brands = []
    return list(dict.fromkeys(brands))


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
    """يرجع الاسم الرسمي المطابق من الكتالوج عند توفره."""
    bv = str(brand_value or "").strip()
    if not bv:
        return ""
    if bv in store_brands:
        return bv
    target_keys = _brand_aliases(bv)
    if not target_keys:
        return bv
    for sb in store_brands:
        if target_keys & _brand_aliases(sb):
            return sb
    return bv


def _concentration_ar(s: str) -> str:
    t = str(s or "").lower()
    if any(x in t for x in ("eau de parfum", "edp")):
        return "أو دو بارفان"
    if any(x in t for x in ("eau de toilette", "edt")):
        return "أو دو تواليت"
    if "parfum" in t:
        return "بارفان"
    return ""


def _build_export_title(raw_name: str, brand: str, gender: str) -> str:
    """عنوان عربي إلزامي يبدأ بكلمة عطر."""
    _sz = safe_float(re.search(r"(\d{2,4})\s*ml", str(raw_name).lower()).group(1), 0) if re.search(r"(\d{2,4})\s*ml", str(raw_name).lower()) else 0
    size_txt = f"{int(_sz)} مل" if _sz > 0 else ""
    conc_txt = _concentration_ar(raw_name)
    line_txt = sanitize_salla_text(str(raw_name or "").strip())
    line_norm = _norm_brand(line_txt)
    for _bk in sorted(_brand_aliases(brand), key=len, reverse=True):
        if _bk:
            line_norm = re.sub(rf"\b{re.escape(_bk)}\b", " ", line_norm, flags=re.I)
    # نُبقي الجزء العربي فقط لضمان عنوان عربي رئيسي بدون إنجليزية
    line_ar = " ".join(re.findall(r"[\u0600-\u06FF]+", line_norm))
    line_ar = sanitize_salla_text(line_ar).strip()
    if not line_ar:
        line_ar = "منتج عطري"
    pieces = ["عطر", sanitize_salla_text(str(brand or "").strip()), line_ar, conc_txt, size_txt, gender]
    title = " ".join([p for p in pieces if p]).strip()
    title = re.sub(r"\s+", " ", title)
    return title[:220]

# رؤوس الأعمدة كما في ملف "منتججديد.csv"
SALLA_SHAMEL_COLUMNS = [
    "النوع ",
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
        return "نسائي"
    if any(x in raw for x in ("رجالي", "رجال", "للرجال", "men", "male", "homme", "pour homme")):
        return "رجالي"
    if any(x in raw for x in ("للجنسين", "unisex", "الجنسين")):
        return "للجنسين"
    return ""


def _safe_alt_text(s: str) -> str:
    """وصف صورة نصي فقط بدون رموز خاصة لتوافق تحقق سلة."""
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
    يُنشئ ملف CSV بترميز UTF-8 مع BOM لاستيراد سلة الشامل.
    مطابق تماماً لتنسيق "منتججديد.csv".
    """
    ncols = len(SALLA_SHAMEL_COLUMNS)

    if missing_df is None or missing_df.empty:
        out = io.StringIO(newline="")
        w = csv.writer(out)
        w.writerow(["﻿بيانات المنتج"] + [""] * (ncols - 1))
        w.writerow(SALLA_SHAMEL_COLUMNS)
        return ("\ufeff" + out.getvalue()).encode("utf-8")

    rows_out = []
    _store_brands = _load_store_brands()
    _store_brands_norm = {_norm_brand(b): b for b in _store_brands if _norm_brand(b)}
    _debug_log("H6", "salla_shamel_export.py:export_start", "Started Salla export", {
        "rows": int(len(missing_df)),
        "store_brands_count": int(len(_store_brands)),
    })
    for _, row in missing_df.iterrows():
        r = row.to_dict()
        raw_pname = _plain_missing_product_name(r)
        gender_inferred = _infer_gender_text(r)
        comp_price = _real_price(r)
        brand = sanitize_salla_text(
            str(r.get("الماركة_الرسمية", "") or r.get("الماركة", "")).strip()
        )
        if brand in ("", "ماركة عالمية", "Unknown", "unknown"):
            brand = ""
        brand = _resolve_brand_to_store(brand, _store_brands)
        pname = _build_export_title(raw_pname, brand, gender_inferred)
        raw_brand = sanitize_salla_text(str(r.get("الماركة", "")).strip())
        exact_brand = sanitize_salla_text(str(r.get("الماركة_الرسمية", "")).strip())
        brand_in_store_exact = bool(brand and brand in _store_brands)
        brand_in_store_norm = bool(_norm_brand(brand) in _store_brands_norm)
        _debug_log("H6", "salla_shamel_export.py:brand_resolution", "Brand resolved for row", {
            "product": pname[:120],
            "raw_brand": raw_brand[:120],
            "exact_brand": exact_brand[:120],
            "final_brand": brand[:120],
            "in_store_exact": brand_in_store_exact,
            "in_store_norm": brand_in_store_norm,
            "store_brand_norm_match": _store_brands_norm.get(_norm_brand(brand), "")[:120],
        })
        img = str(r.get("صورة_المنافس", r.get("image_url", ""))).strip()
        sku = _real_sku(r)
        
        # استخراج المكونات وتنسيق الوصف بأسلوب مهووس
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
        
        category = sanitize_salla_text(
            str(r.get("التصنيف_الرسمي", "") or r.get("تصنيف المنتج", "")).strip()
        )
        if not category:
            category = auto_infer_category(pname, str(r.get("الجنس", "")))
        if not str(r.get("التصنيف_الرسمي", "")).strip():
            if gender_inferred == "نسائي":
                category = "العطور > عطور نسائية"
            elif gender_inferred == "رجالي":
                category = "العطور > عطور رجالية"
            elif gender_inferred == "للجنسين":
                category = "العطور > عطور للجنسين"
            else:
                category = "العطور > عطور للجنسين"
        # إذا AI فشل بالمطابقة الدقيقة نتركه فارغاً لتجنب رفض الاستيراد بسبب تصنيف غير معرف
        if category in ("", "العطور", "عطور للجنسين", "غير محدد", "منتجات عامة"):
            category = ""
        alt_txt = _safe_alt_text(f"صورة {pname}")
        _debug_log("H7", "salla_shamel_export.py:title_category_probe", "Row title/category probe", {
            "product_raw": str(r.get("منتج_المنافس", ""))[:180],
            "pname_export": pname[:180],
            "pname_has_ar": _contains_arabic(pname),
            "gender_inferred": gender_inferred,
            "category_final": category,
            "category_source_exact": bool(str(r.get("التصنيف_الرسمي", "")).strip()),
        })

        final_desc = sanitize_salla_text(str(r.get("وصف_AI", "") or "").strip())
        if not final_desc:
            final_desc = desc_text

        row_csv = {
            "النوع ": "منتج",
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
            "العنوان الترويجي": "",
            "تثبيت المنتج": "",
            "الباركود": "",
            "السعرات الحرارية": "",
            "MPN": "",
            "GTIN": "",
            "خاضع للضريبة ؟": "نعم",
            "سبب عدم الخضوع للضريبة": "",
            "[1] الاسم": "", "[1] النوع": "", "[1] القيمة": "", "[1] الصورة / اللون": "",
            "[2] الاسم": "", "[2] النوع": "", "[2] القيمة": "", "[2] الصورة / اللون": "",
            "[3] الاسم": "", "[3] النوع": "", "[3] القيمة": "", "[3] الصورة / اللون": "",
        }
        rows_out.append([row_csv.get(c, "") for c in SALLA_SHAMEL_COLUMNS])

    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    # إضافة BOM يدوياً في أول سطر لضمان توافق سلة
    writer.writerow(["﻿بيانات المنتج"] + [""] * (ncols - 1))
    writer.writerow(SALLA_SHAMEL_COLUMNS)
    for line in rows_out:
        writer.writerow(line)
    
    return ("\ufeff" + buf.getvalue()).encode("utf-8")
