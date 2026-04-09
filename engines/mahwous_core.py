"""
mahwous_core — فلاتر مسار صارمة، استخراج المكونات، وتنسيق "مهووس" الاحترافي.
متوافق 100% مع منصة سلة و Make.
"""
from __future__ import annotations

import re
import html
from typing import Any, Dict, List, Tuple

import pandas as pd

try:
    from config import REJECT_KEYWORDS
except ImportError:
    REJECT_KEYWORDS = [
        "sample", "عينة", "عينه", "decant", "تقسيم", "تقسيمة",
        "split", "miniature", "0.5ml", "1ml", "2ml", "3ml",
    ]

# تعبيرات نمطية لتنظيف النصوص لمنصة سلة
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_NON_TEXT_RE = re.compile(r"[^\w\s\.\-\(\)\[\]\!\؟\:\,\|\/]")

def _safe_float(val: Any, default: float = 0.0) -> float:
    try:
        if val is None or str(val).strip() in ("", "nan", "None", "NaN"):
            return default
        return float(str(val).replace(",", ""))
    except (ValueError, TypeError):
        return default

def _is_sample_strict(name: str) -> bool:
    if not isinstance(name, str) or not name.strip():
        return True
    nl = name.lower()
    return any(k.lower() in nl for k in REJECT_KEYWORDS)

def _extract_ml(name: str) -> float:
    if not isinstance(name, str):
        return -1.0
    m = re.search(r"(\d+(?:\.\d+)?)\s*(?:ml|مل|ملي)\b", name, re.I)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            return -1.0
    return -1.0

def _classify_rejected(name: str) -> bool:
    if not isinstance(name, str):
        return True
    nl = name.lower()
    rejects = ["sample", "عينة", "عينه", "miniature", "مينياتشر", "travel size", "decant", "تقسيم", "split"]
    return any(w in nl for w in rejects)

def apply_strict_pipeline_filters(
    df: pd.DataFrame, name_col: str = "منتج_المنافس"
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    if df is None or df.empty:
        return df, {"dropped": 0}

    if name_col not in df.columns:
        # محاولة البحث عن عمود الاسم البديل
        alt_cols = ["المنتج", "اسم المنتج", "Product", "Name"]
        for c in alt_cols:
            if c in df.columns:
                name_col = c
                break
        else:
            return df.copy(), {"dropped": 0, "warning": f"عمود غير موجود: {name_col}"}

    stats: Dict[str, Any] = {
        "dropped_sample_kw": 0,
        "dropped_small_ml": 0,
        "dropped_class_rejected": 0,
        "dropped_empty_name": 0,
    }
    keep_idx: List[Any] = []

    for idx, row in df.iterrows():
        name = str(row.get(name_col, "")).strip()
        if not name or name.lower() in ("nan", "none", "<na>"):
            stats["dropped_empty_name"] += 1
            continue
        if _is_sample_strict(name):
            stats["dropped_sample_kw"] += 1
            continue
        if _classify_rejected(name):
            stats["dropped_class_rejected"] += 1
            continue
        ml = _extract_ml(name)
        if 0 < ml < 5:
            stats["dropped_small_ml"] += 1
            continue

        keep_idx.append(idx)

    out = df.loc[keep_idx].reset_index(drop=True) if keep_idx else pd.DataFrame()
    stats["dropped"] = len(df) - len(out)
    stats["kept"] = len(out)
    return out, stats

def sanitize_salla_text(text: str) -> str:
    """تنظيف النصوص من الرموز البرمجية والأحرف الخاصة المعيقة للرفع لسلة."""
    if not text: return ""
    # إزالة الـ HTML
    text = _HTML_TAG_RE.sub(" ", str(text))
    # فك ترميز HTML entities
    text = html.unescape(text)
    # إزالة الأحرف الغريبة مع الحفاظ على العربية والإنجليزية والترقيم الأساسي
    # text = _NON_TEXT_RE.sub("", text)
    # تنظيف المسافات الزائدة
    return re.sub(r"\s+", " ", text).strip()

def format_mahwous_description(product_data: dict) -> str:
    """تنسيق الوصف بأسلوب مهووس الاحترافي."""
    name = sanitize_salla_text(product_data.get("name", "عطر فاخر"))
    brand = sanitize_salla_text(product_data.get("brand", "ماركة عالمية"))
    desc = product_data.get("description", "")
    notes = product_data.get("notes", {}) # الهرم العطري
    features = product_data.get("features", [
        "عطر أصلي 100% بضمان متجر مهووس",
        "ثبات عالي وفوحان يأسر الحواس",
        "مناسب للاستخدام اليومي والمناسبات الخاصة"
    ])

    # بناء الوصف بتنسيق مهووس
    lines = [
        f"## {name} من {brand}",
        f"\nاستمتع بتجربة عطرية فريدة مع {name}، العطر الذي يجسد الأناقة والفخامة في كل رشة. متوفر الآن في متجر مهووس، وجهتك الأولى لأرقى العطور العالمية.",
        "\n### ✨ مميزات المنتج",
    ]
    
    for feat in features:
        lines.append(f"* {sanitize_salla_text(feat)}")
    
    if notes:
        lines.append("\n### 🎼 الهرم العطري (المكونات الحقيقية)")
        if notes.get("top"): lines.append(f"* **الافتتاحية:** {sanitize_salla_text(notes['top'])}")
        if notes.get("heart"): lines.append(f"* **القلب:** {sanitize_salla_text(notes['heart'])}")
        if notes.get("base"): lines.append(f"* **القاعدة:** {sanitize_salla_text(notes['base'])}")
    elif desc:
        lines.append("\n### 📝 وصف العطر")
        lines.append(sanitize_salla_text(desc))

    lines.append("\n---\n*جميع عطورنا أصلية 100% ونضمن لك الجودة والتميز في كل طلب.*")
    
    return "\n".join(lines)

def validate_export_product_dataframe(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    issues: List[str] = []
    if df is None or df.empty:
        return False, ["لا توجد بيانات للتحقق أو التصدير."]

    for i, (_, row) in enumerate(df.iterrows()):
        name = (
            str(row.get("منتج_المنافس", "")).strip()
            or str(row.get("المنتج", "")).strip()
            or str(row.get("أسم المنتج", "")).strip()
            or str(row.get("اسم المنتج", "")).strip()
        )
        price = _safe_float(
            row.get("سعر_المنافس", row.get("سعر المنافس", row.get("السعر", 0)))
        )
        label = name[:48] + ("…" if len(name) > 48 else "") if name else "(بدون اسم)"

        if not name or name.lower() in ("nan", "none"):
            issues.append(f"صف {i + 1}: اسم المنتج فارغ — {label}")
        if price <= 0:
            issues.append(f"صف {i + 1}: السعر غير صالح أو صفر ({price}) — {label}")

    return (len(issues) == 0, issues)
