"""
تصدير منتجات مفقودة إلى CSV متوافق مع قالب استيراد منصة سلة (40 عموداً بالترتيب الحرفي).

- عمود «النوع » يحتفظ بالمسافة الزائدة كما في قالب سلة الرسمي.
- التصدير عبر pandas.to_csv مع encoding=utf-8-sig و quoting=csv.QUOTE_ALL.
"""
from __future__ import annotations

import csv
import difflib
import html
import io
import re
from typing import Any

import pandas as pd

from engines.ai_engine import auto_infer_category
from engines.mahwous_core import sanitize_salla_text
from engines.prompts import (
    SALLA_BRANDS_FILE,
    SALLA_BRANDS_COL,
    BRANDS_CSV_FILE,
    SALLA_CATEGORIES_FILE,
    SALLA_CATEGORIES_COL,
    CATEGORIES_CSV_FILE,
)
from utils.data_paths import get_catalog_data_path
from utils.data_sanitizer import get_brand_arabic_name
from utils.helpers import safe_float

# ── مخطط سلة الشامل: 40 عموداً بالترتيب الحرفي (مطابقة لقالب الاستيراد) ─────────
SALLA_SHAMEL_COLUMNS: list[str] = [
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
    "[3] الصورة / اللون",
]

# أول وسم HTML مسموح به داخل عمود «الوصف» في CSV سلة (إزالة أي نص حواري يسبق النموذج)
_HTML_DESCRIPTION_START = re.compile(r"(?is)<\s*(?:h2|div)\b")


def sanitize_salla_description_html(raw: str) -> str:
    """
    يزيل أي نص حواري أو شروحات تسبق أول وسم <h2> أو <div> في مخرجات قديمة أو LLM.
    عمود الوصف في الإنتاج يجب أن يبقى HTML خام فقط.
    """
    if raw is None:
        return ""
    s = str(raw).strip()
    if not s:
        return ""
    s = re.sub(r"(?is)^```(?:html|xml)?\s*", "", s).strip()
    s = re.sub(r"(?is)\s*```\s*$", "", s).strip()
    m = _HTML_DESCRIPTION_START.search(s)
    if not m:
        return ""
    return s[m.start() :].strip()


def generate_salla_html_description(product_name: str, brand_name: str = "غير متوفر") -> str:
    """
    قالب HTML ثابت لمتجر مهووس — يُحقن الاسم والماركة عبر f-strings فقط (بدون LLM).
    الهيكل الإلزامي: <h2> ثم <p> ثم <ul> ثم فقرات ختامية.
    """
    pn = sanitize_salla_text(str(product_name or "").strip()) or "منتج"
    br = sanitize_salla_text(str(brand_name or "").strip()) or "غير متوفر"
    safe_name = html.escape(pn, quote=True)
    safe_brand = html.escape(br, quote=True)
    return (
        f"<h2>عطر {safe_name} أو دو بارفيوم للجنسين</h2>\n"
        f"<p>اكتشف سحر <strong>عطر {safe_name}</strong> — عطر فاخر يجمع بين الأصالة والتميز؛ "
        f"ماركة <strong>{safe_brand}</strong>.</p>\n"
        "<ul>\n"
        f"<li><strong>الماركة:</strong> {safe_brand}</li>\n"
        f"<li><strong>الاسم:</strong> {safe_name}</li>\n"
        "<li><strong>الجنس:</strong> للجنسين</li>\n"
        "<li><strong>التركيز:</strong> أو دو بارفيوم</li>\n"
        "<li><strong>الثبات والفوحان:</strong> تركيز أو دو بارفيوم يضمن فوحاناً يدوم طويلاً.</li>\n"
        "<li><strong>القيمة:</strong> عطر فاخر بسعر مناسب من متجر مهووس الموثوق.</li>\n"
        "</ul>\n"
        '<p><a href="https://mahwous.com/categories/mens-perfumes" target="_blank" rel="noopener">'
        "عطور رجالية</a> | "
        '<a href="https://mahwous.com/categories/womens-perfumes" target="_blank" rel="noopener">'
        "عطور نسائية</a></p>\n"
        "<p><strong>عالمك العطري يبدأ من مهووس.</strong> أصلي 100% | شحن سريع داخل السعودية.</p>"
    )


def build_salla_shamel_description_html(
    product_name: str,
    brand_raw: str,
    *,
    resolved_brand: str | None = None,
) -> str:
    """يُنشئ HTML الوصف للتصدير: قالب ثابت + مطابقة ماركة المتجر + تنظيف أي بقايا نصية."""
    br_hint = sanitize_salla_text(str(brand_raw or "").strip())
    if resolved_brand is None:
        brand_resolved = resolve_brand_for_shamel(br_hint) if br_hint else ""
    else:
        brand_resolved = str(resolved_brand).strip()
    brand_final = brand_resolved or (br_hint if br_hint else "غير متوفر")
    raw_html = generate_salla_html_description(product_name, brand_final)
    return sanitize_salla_description_html(raw_html)


def _is_url_text(s: Any) -> bool:
    t = str(s or "").strip().lower()
    return t.startswith("http://") or t.startswith("https://")


def _extract_product_name(row: dict[str, Any]) -> str:
    for key in (
        "أسم المنتج",
        "اسم المنتج",
        "اسم_المنتج",
        "المنتج",
        "منتج_المنافس",
        "المنتج_المنافس",
        "cleaned_title",
        "Product",
        "Name",
        "name",
        "title",
        "الاسم",
    ):
        v = row.get(key)
        if v is None or (isinstance(v, float) and pd.isna(v)):
            continue
        s = sanitize_salla_text(str(v).strip())
        if s and not _is_url_text(s):
            return s
    return ""


def _extract_image_url(row: dict[str, Any]) -> str:
    for key in ("صورة_المنافس", "صورة المنتج", "image_url", "صورة", "الصورة"):
        v = row.get(key)
        if v is None or (isinstance(v, float) and pd.isna(v)):
            continue
        s = str(v).strip()
        if s and s.lower() not in ("nan", "none"):
            return s
    return ""


def _extract_price_string(row: dict[str, Any]) -> str:
    for k in ("سعر_المنافس", "سعر المنافس", "السعر", "سعر المنتج", "Price", "price", "PRICE"):
        if k not in row:
            continue
        p = safe_float(row.get(k), 0.0)
        if p > 0:
            return str(round(p, 2))
    return ""


def _brand_hint_for_description(row: dict[str, Any]) -> str:
    for key in ("الماركة_الرسمية", "الماركة", "brand", "Brand"):
        v = row.get(key)
        if v is None or (isinstance(v, float) and pd.isna(v)):
            continue
        s = sanitize_salla_text(str(v).strip())
        if s and s.lower() not in ("nan", "none", "unknown", "ماركة عالمية"):
            return s
    return "غير متوفر"


def _extract_gender_hint(row: dict[str, Any]) -> str:
    for key in ("الجنس", "gender_hint", "Gender", "gender"):
        v = row.get(key)
        if v is None or (isinstance(v, float) and pd.isna(v)):
            continue
        s = str(v).strip()
        if s and s.lower() not in ("nan", "none"):
            return s
    return ""


def _extract_category_raw(row: dict[str, Any]) -> str:
    for key in (
        "التصنيف_الرسمي",
        "تصنيف المنتج",
        "التصنيف",
        "category",
        "Category",
    ):
        v = row.get(key)
        if v is None or (isinstance(v, float) and pd.isna(v)):
            continue
        s = sanitize_salla_text(str(v).strip())
        if s and s.lower() not in ("nan", "none"):
            return s
    return ""


def build_salla_shamel_dataframe(missing_df: pd.DataFrame) -> pd.DataFrame:
    """يبني DataFrame بأعمدة سلة الـ 40 من جدول المنتجات المفقودة."""
    if missing_df is None or missing_df.empty:
        return pd.DataFrame(columns=SALLA_SHAMEL_COLUMNS)

    rows: list[dict[str, Any]] = []
    for _, row in missing_df.iterrows():
        r = row.to_dict()
        product_name = _extract_product_name(r)
        image_url = _extract_image_url(r)
        price_str = _extract_price_string(r)
        brand_desc = _brand_hint_for_description(r)
        gender_hint = _extract_gender_hint(r)
        cat_raw = _extract_category_raw(r)

        cat_resolved = (
            resolve_category_for_shamel(
                cat_raw,
                gender_hint=gender_hint,
                product_name_fallback=product_name,
            )
            if cat_raw or product_name
            else ""
        )
        brand_salla = resolve_brand_for_shamel(brand_desc) if brand_desc and brand_desc != "غير متوفر" else ""

        description_html = build_salla_shamel_description_html(
            product_name,
            brand_desc,
            resolved_brand=brand_salla,
        )
        image_caption = f"زجاجة عطر {product_name} الأصلية" if product_name else ""

        row_out: dict[str, Any] = {c: "" for c in SALLA_SHAMEL_COLUMNS}
        row_out["النوع "] = "منتج"
        row_out["أسم المنتج"] = product_name
        row_out["تصنيف المنتج"] = cat_resolved
        row_out["صورة المنتج"] = image_url
        row_out["وصف صورة المنتج"] = image_caption
        row_out["نوع المنتج"] = "منتج جاهز"
        row_out["سعر المنتج"] = price_str
        row_out["الوصف"] = description_html
        row_out["هل يتطلب شحن؟"] = "نعم"
        row_out["رمز المنتج sku"] = ""
        row_out["سعر التكلفة"] = ""
        row_out["السعر المخفض"] = ""
        row_out["تاريخ بداية التخفيض"] = ""
        row_out["تاريخ نهاية التخفيض"] = ""
        row_out["اقصي كمية لكل عميل"] = 0
        row_out["إخفاء خيار تحديد الكمية"] = 0
        row_out["اضافة صورة عند الطلب"] = 0
        row_out["الوزن"] = 0.2
        row_out["وحدة الوزن"] = "kg"
        row_out["الماركة"] = brand_salla
        row_out["العنوان الترويجي"] = ""
        row_out["تثبيت المنتج"] = ""
        row_out["الباركود"] = ""
        row_out["السعرات الحرارية"] = ""
        row_out["MPN"] = ""
        row_out["GTIN"] = ""
        row_out["خاضع للضريبة ؟"] = "نعم"
        row_out["سبب عدم الخضوع للضريبة"] = ""
        row_out["[1] الاسم"] = ""
        row_out["[1] النوع"] = ""
        row_out["[1] القيمة"] = ""
        row_out["[1] الصورة / اللون"] = ""
        row_out["[2] الاسم"] = ""
        row_out["[2] النوع"] = ""
        row_out["[2] القيمة"] = ""
        row_out["[2] الصورة / اللون"] = ""
        row_out["[3] الاسم"] = ""
        row_out["[3] النوع"] = ""
        row_out["[3] القيمة"] = ""
        row_out["[3] الصورة / اللون"] = ""

        rows.append(row_out)

    return pd.DataFrame(rows, columns=SALLA_SHAMEL_COLUMNS)


def export_to_salla_shamel(missing_df: pd.DataFrame, generate_descriptions: bool = True) -> bytes:
    """
    يصدّر جدول المنتجات المفقودة إلى بايتات CSV (utf-8-sig + اقتباس جميع الحقول).

    المعامل generate_descriptions يُحتفظ للتوافق مع الاستدعاءات القديمة فقط؛
    عمود «الوصف» يُبنى دائماً من القالب الثابت (generate_salla_html_description)
    مع حقن الاسم والماركة وتمرير sanitize_salla_description_html — لا يُحقن نص LLM أو وصف_AI.
    """
    _ = generate_descriptions
    df = build_salla_shamel_dataframe(missing_df)
    if not df.empty:
        df = df.reindex(columns=SALLA_SHAMEL_COLUMNS)
    buf = io.BytesIO()
    df.to_csv(
        buf,
        index=False,
        encoding="utf-8-sig",
        quoting=csv.QUOTE_ALL,
        lineterminator="\n",
    )
    return buf.getvalue()


# ═══════════════════════════════════════════════════════════════════════════════
#  مطابقة الماركات والتصنيفات مع كتالوج المتجر (يُستدعى من engines.ai_engine وغيره)
# ═══════════════════════════════════════════════════════════════════════════════


def _norm_brand(s: str) -> str:
    t = sanitize_salla_text(str(s or "")).strip().lower()
    t = re.sub(r"[\|\-_/]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _load_store_brands() -> list[str]:
    brands: list[str] = []
    candidate_files = [
        (SALLA_BRANDS_FILE.replace(" ", "_"), SALLA_BRANDS_COL),
        (SALLA_BRANDS_FILE, SALLA_BRANDS_COL),
        (BRANDS_CSV_FILE, None),
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
    cats: list[str] = []
    candidate_files = [
        (SALLA_CATEGORIES_FILE.replace(" ", "_"), SALLA_CATEGORIES_COL),
        (SALLA_CATEGORIES_FILE, SALLA_CATEGORIES_COL),
        (CATEGORIES_CSV_FILE, None),
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
    s = str(brand_label or "").strip()
    if not s:
        return set()
    parts = [p.strip() for p in re.split(r"[|/\\\-]+", s) if p.strip()]
    keys = {_norm_brand(s)}
    keys.update({_norm_brand(p) for p in parts})
    return {k for k in keys if k}


def _resolve_brand_to_store(brand_value: str, store_brands: list[str]) -> str:
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
    bv_lower = bv.lower().strip()
    for sb in store_brands:
        parts = [p.strip().lower() for p in _re.split(r"[|/\\]", sb.lower()) if p.strip()]
        if bv_lower in parts or any(bv_lower == p or bv_lower in p.split() for p in parts):
            return sb
    return ""


def _norm_category(s: str) -> str:
    t = sanitize_salla_text(str(s or "")).strip().lower()
    t = re.sub(r"[>|\-_/]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _resolve_category_to_store(cat_value: str, store_categories: list[str], gender_hint: str = "") -> str:
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

    return ""


def resolve_brand_for_shamel(brand_raw: str) -> str:
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

    r: str = ""
    for cand in _cands:
        r = _try(cand)
        if r:
            return r

    if gh == "للجنسين" and not r:
        for c in store:
            if c.strip() == "عطور للجنسين":
                return c
        if "العطور" in store:
            return "العطور"

    return ""
