"""
utils/product_analyzer.py — وحدة التحليل الموضعي للمنتجات (الوحدة الثالثة)
═══════════════════════════════════════════════════════════════════════════════
✅ تحليل شامل للمنتج: سعر + تطابق + قسم صحيح + توصية
✅ يُستدعى من زر [📊 تحليل المنتج] في بطاقة كل منتج
✅ يفحص: الكتالوج + بيانات المنافسين + يصحح الأخطاء
✅ نتيجة منظمة وقابلة للعرض مباشرة في Streamlit
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Optional
import pandas as pd


# ══════════════════════════════════════════════════════════════════════════════
#  التحليل الموضعي الرئيسي
# ══════════════════════════════════════════════════════════════════════════════

def analyze_product_inline(
    our_name: str,
    our_price: float,
    comp_name: str,
    comp_price: float,
    comp_source: str,
    match_pct: float,
    brand: str = "",
    section: str = "general",
    results_df: Optional[pd.DataFrame] = None,
) -> dict:
    """
    يُجري تحليلاً شاملاً لمنتج بعينه عند الضغط على [📊 تحليل المنتج].

    الخطوات:
    1. يستدعي ai_deep_analysis للتحليل الذكي
    2. يفحص هل المنتج في القسم الصحيح
    3. يقترح السعر الأمثل
    4. يرجع dict جاهزاً للعرض

    Returns:
        {
            "success": bool,
            "match_valid": bool,
            "correct_section": str,
            "suggested_price": float,
            "analysis": str,
            "price_verdict": str,
            "recommendation": str,
            "error": str | None
        }
    """
    result = {
        "success": False,
        "match_valid": None,
        "correct_section": "",
        "suggested_price": 0.0,
        "analysis": "",
        "price_verdict": "",
        "recommendation": "",
        "confidence": 0,
        "error": None,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
    }

    try:
        from engines.ai_engine import ai_deep_analysis, suggest_price, verify_match
    except ImportError as e:
        result["error"] = f"تعذّر تحميل AI: {e}"
        return result

    # ── 1. التحليل العميق ───────────────────────────────────────────────
    try:
        analysis_raw = ai_deep_analysis(
            our_product=our_name,
            our_price=our_price,
            comp_product=comp_name,
            comp_price=comp_price,
            section=section,
            brand=brand,
        )
        if isinstance(analysis_raw, dict):
            result["analysis"]   = str(analysis_raw.get("response", analysis_raw.get("analysis", "")))
            result["confidence"] = int(analysis_raw.get("confidence", match_pct))
        else:
            result["analysis"] = str(analysis_raw)
    except Exception as e:
        result["analysis"] = f"[تعذّر التحليل: {e}]"

    # ── 2. التحقق من صحة المطابقة والقسم ───────────────────────────────
    try:
        verify_r = verify_match(our_name, comp_name, our_price, comp_price)
        if verify_r.get("success"):
            result["match_valid"]     = bool(verify_r.get("match", True))
            result["correct_section"] = str(verify_r.get("correct_section", ""))
            result["confidence"]      = int(verify_r.get("confidence", match_pct))
    except Exception:
        pass

    # ── 3. اقتراح السعر الأمثل ──────────────────────────────────────────
    try:
        price_r = suggest_price(our_name, our_price, comp_price, section)
        if isinstance(price_r, dict) and price_r.get("success"):
            result["suggested_price"] = float(price_r.get("suggested_price", 0) or 0)
            result["recommendation"]  = str(price_r.get("recommendation", ""))
        elif isinstance(price_r, (int, float)):
            result["suggested_price"] = float(price_r)
    except Exception:
        pass

    # ── 4. حكم السعر ────────────────────────────────────────────────────
    if our_price > 0 and comp_price > 0:
        diff    = our_price - comp_price
        diff_pc = diff / comp_price * 100
        if diff > 5:
            result["price_verdict"] = (
                f"🔴 سعرنا أعلى بـ {diff:.0f} ر.س ({diff_pc:.1f}%) — "
                "ننصح بخفضه أو تبريره بجودة أعلى"
            )
        elif diff < -5:
            result["price_verdict"] = (
                f"🟢 سعرنا أقل بـ {abs(diff):.0f} ر.س ({abs(diff_pc):.1f}%) — "
                "فرصة لرفع هامش الربح"
            )
        else:
            result["price_verdict"] = (
                f"✅ سعرنا تنافسي (فرق {diff:+.0f} ر.س)"
            )

    # ── 5. فحص القسم الصحيح ─────────────────────────────────────────────
    if result["correct_section"]:
        section_map = {
            "raise":    "🔴 سعر أعلى",
            "lower":    "🟢 سعر أقل",
            "approved": "✅ موافق عليها",
            "missing":  "🔍 منتجات مفقودة",
            "review":   "⚠️ تحت المراجعة",
            "excluded": "⚪ مستبعد",
        }
        cur_ar  = section_map.get(section, section)
        corr_ar = result["correct_section"]
        if corr_ar and corr_ar != cur_ar:
            result["section_mismatch"] = (
                f"⚠️ المنتج في **{cur_ar}** لكن AI يرى أنه ينتمي لـ **{corr_ar}**"
            )
        else:
            result["section_mismatch"] = ""
    else:
        result["section_mismatch"] = ""

    result["success"] = True
    return result


# ══════════════════════════════════════════════════════════════════════════════
#  عرض نتيجة التحليل في Streamlit
# ══════════════════════════════════════════════════════════════════════════════

def render_analysis_result(result: dict, container=None) -> None:
    """
    يعرض نتيجة analyze_product_inline() داخل Streamlit.
    يُمرَّر container=st إذا أردت عرضه في مستوى الصفحة،
    أو container=st.expander(...) لعرضه داخل مُوسّع.
    """
    import streamlit as st
    c = container or st

    if not result.get("success"):
        c.error(f"❌ {result.get('error', 'فشل التحليل')}")
        return

    # حكم السعر
    if result.get("price_verdict"):
        c.markdown(
            f'<div style="background:#0d1b2a;border-radius:8px;padding:10px 14px;'
            f'font-size:.88rem;margin-bottom:6px">'
            f'{result["price_verdict"]}</div>',
            unsafe_allow_html=True,
        )

    # تحذير القسم
    if result.get("section_mismatch"):
        c.warning(result["section_mismatch"])

    # تحليل AI
    if result.get("analysis"):
        c.markdown(
            f'<div style="background:#091929;border:1px solid #1e3a5f;'
            f'border-radius:8px;padding:12px 14px;font-size:.85rem;'
            f'line-height:1.7;white-space:pre-wrap">'
            f'🤖 <b>تحليل AI:</b><br>{result["analysis"][:600]}</div>',
            unsafe_allow_html=True,
        )

    # السعر المقترح
    sp = result.get("suggested_price", 0)
    if sp and sp > 0:
        c.success(f"💰 **السعر المقترح: {sp:,.0f} ر.س**")
        if result.get("recommendation"):
            c.caption(result["recommendation"][:200])

    # مصداقية المطابقة
    if result.get("match_valid") is False:
        c.error("🔵 AI: المطابقة خاطئة — يجب نقل المنتج للمنتجات المفقودة")
    elif result.get("match_valid") is True:
        c.success(f"✅ AI: مطابقة صحيحة ({result.get('confidence', 0)}%)")
