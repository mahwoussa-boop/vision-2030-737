"""
تصدير منتجات مفقودة بتنسيق CSV سلة الشامل المحدث.
مطابق تماماً لنموذج "منتججديد.csv" المرفق.
v28.0 - النسخة الكاملة المدمجة.
"""
import html
import io
import csv
import re
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional

import pandas as pd

from engines.ai_engine import auto_infer_category, generate_mahwous_description
from engines.mahwous_core import sanitize_salla_text, format_mahwous_description
from utils.helpers import safe_float

_HTML_TAG_RE = re.compile(r"<[^>]+>")
_ALT_SAFE_RE = re.compile(r"[^0-9A-Za-z\u0600-\u06FF\s]")

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
    for _, row in missing_df.iterrows():
        r = row.to_dict()
        pname = _plain_missing_product_name(r)
        comp_price = _real_price(r)
        brand = sanitize_salla_text(
            str(r.get("الماركة_الرسمية", "") or r.get("الماركة", "")).strip()
        )
        if brand in ("", "ماركة عالمية", "Unknown", "unknown"):
            brand = ""
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
        # إذا AI فشل بالمطابقة الدقيقة نتركه فارغاً لتجنب رفض الاستيراد بسبب تصنيف غير معرف
        if category in ("", "العطور", "عطور للجنسين", "غير محدد", "منتجات عامة"):
            category = ""
        alt_txt = _safe_alt_text(f"صورة {pname}")

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
