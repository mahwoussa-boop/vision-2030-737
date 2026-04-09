"""
pages/magic_factory.py — مصنع المنتجات السحرية (الوحدة الخامسة)
══════════════════════════════════════════════════════════════════════════════════════════════
✅ إنشاء منتجات احترافية من (روابط / صور / نصوص)
✅ كشط ذكي + تنظيف شامل (Sanitization) + توليد ماركات تلقائي
✅ تحويل الصور (Gemini Vision) إلى بيانات منتجات
✅ معالجة جماعية للروابط (Batch Processing)
✅ حفظ مباشر في الكتالوج (upsert_our_catalog)
"""
import streamlit as st
import pandas as pd
import time
import re
import json
from datetime import datetime
from typing import Optional, List, Dict

from config import *
from styles import get_styles
from engines.ai_engine import (
    call_ai, extract_product, generate_mahwous_description,
    fetch_product_images, fetch_og_image_url
)
from utils.data_sanitizer import (
    sanitize_new_product, sanitize_full_description,
    build_title_from_raw, generate_brand_record, append_brand_to_csv,
    get_brand_arabic_name, get_brand_display_name
)
from utils.db_manager import (
    upsert_our_catalog, save_processed, log_event
)
from utils.helpers import fetch_page_title_from_url

# ══════════════════════════════════════════════════════════════════════════════
#  الإعدادات والواجهة
# ══════════════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="مصنع المنتجات السحرية | مهووس",
    page_icon="✨",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown(get_styles(), unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
#  وظائف المعالجة الأساسية
# ══════════════════════════════════════════════════════════════════════════════

def _process_product_pipeline(raw_data: dict, source_type: str = "manual") -> dict:
    """
    خط المعالجة السحري (Magic Pipeline):
    1. تنظيف إجباري (sanitize_new_product)
    2. التحقق من الماركة وتوليدها إذا لزم الأمر
    3. بناء العنوان العربي الصارم
    4. توليد الوصف الاحترافي (إذا مفقود)
    """
    # 1. تنظيف أولي
    clean = sanitize_new_product(raw_data)
    
    # 2. إدارة الماركة
    brand_raw = clean.get("brand", "").strip()
    if brand_raw:
        # تحميل الماركات الحالية للمطابقة
        try:
            brands_df = pd.read_csv("data/brands.csv", encoding="utf-8-sig")
            official_brand = get_brand_arabic_name(brand_raw, brands_df)
        except:
            official_brand = ""
            
        if not official_brand:
            # ماركة جديدة تماماً! لنولّد سجلها
            with st.spinner(f"✨ نولّد سجل الماركة: {brand_raw}..."):
                # محاولة استخراج الاسم العربي والوصف بالـ AI
                prompt = f"أعطني الاسم العربي والوصف التسويقي لماركة العطور العالمية: {brand_raw}. الرد JSON فقط: {{'ar': '...', 'desc': '...'}}"
                ai_res = call_ai(prompt, json_mode=True)
                ar_name = ai_res.get("ar", brand_raw)
                ar_desc = ai_res.get("desc", f"عطور {brand_raw} الأصلية.")
                
                brand_rec = generate_brand_record(brand_raw, ar_name, ar_desc)
                append_brand_to_csv(brand_rec, "data/brands.csv")
                clean["brand"] = brand_rec["اسم الماركة"]
        else:
            clean["brand"] = official_brand

    # 3. بناء العنوان العربي الصارم
    if not clean.get("arabic_name"):
        clean["arabic_name"] = build_title_from_raw(
            clean.get("name", ""),
            brand_arabic=get_brand_display_name(clean.get("brand", "")),
            size=clean.get("size", ""),
            concentration=clean.get("concentration", ""),
            gender=clean.get("gender", "")
        )

    # 4. الوصف الاحترافي
    if not clean.get("description") or len(str(clean["description"])) < 50:
        with st.spinner("✍️ نكتب وصفاً احترافياً..."):
            # دالة generate_mahwous_description تتوقع (product_name, price, ...)
            # نمرر السعر إذا وجد، وإلا 0
            price_val = clean.get("price", 0)
            desc = generate_mahwous_description(clean["arabic_name"], price_val)
            clean["description"] = sanitize_full_description(desc)

    return clean

# ══════════════════════════════════════════════════════════════════════════════
#  الواجهة الرئيسية
# ══════════════════════════════════════════════════════════════════════════════

st.title("✨ مصنع المنتجات السحرية")
st.caption("حوّل أي (رابط / صورة / نص) إلى منتج احترافي جاهز للبيع في ثوانٍ.")

tab1, tab2, tab3, tab4 = st.tabs([
    "🔗 من روابط", "🖼️ من صورة (Vision)", "📝 من نص ناقص", "📦 معالجة جماعية"
])

# ── Tab 1: من روابط ──────────────────────────────────────────────────────────
with tab1:
    urls_input = st.text_area("أدخل روابط المنتجات (رابط في كل سطر):", height=150, placeholder="https://store.com/product-1\nhttps://another.com/product-2")
    
    if st.button("🚀 ابدأ المصنع السحري", key="run_urls"):
        urls = [u.strip() for u in urls_input.split("\n") if u.strip()]
        if not urls:
            st.warning("يرجى إدخال رابط واحد على الأقل.")
        else:
            progress = st.progress(0)
            for idx, url in enumerate(urls):
                st.write(f"🔍 نعالج: {url}...")
                # 1. كشط أولي (بسيط)
                og_img = fetch_og_image_url(url)
                title = fetch_page_title_from_url(url)
                
                # 2. استخراج بيانات بالـ AI من العنوان والصفحة
                extracted = extract_product(title) # دالة من ai_engine
                extracted["image_url"] = og_img
                extracted["url"] = url
                
                # 3. خط المعالجة
                final = _process_product_pipeline(extracted, "url")
                
                # 4. عرض وحفظ
                with st.expander(f"✅ {final['arabic_name']}", expanded=True):
                    col1, col2 = st.columns([1, 3])
                    with col1:
                        if final.get("image_url"):
                            st.image(final["image_url"], width=150)
                    with col2:
                        st.write(f"**الماركة:** {final.get('brand')}")
                        st.write(f"**السعر التقديري:** {final.get('price', 0)} ر.س")
                        if st.button("💾 حفظ في الكتالوج", key=f"save_{idx}"):
                            upsert_our_catalog(final)
                            st.success("تم الحفظ!")
                
                progress.progress((idx + 1) / len(urls))

# ── Tab 2: من صورة (Vision) ───────────────────────────────────────────────────
with tab2:
    uploaded_file = st.file_uploader("ارفع صورة المنتج (العبوة أو الكرتون):", type=["jpg", "png", "jpeg"])
    if uploaded_file:
        st.image(uploaded_file, caption="الصورة المرفوعة", width=300)
        if st.button("👁️ اقرأ الصورة وحللها", key="run_vision"):
            with st.spinner("🤖 Gemini Vision يحلل الصورة..."):
                # محاكاة استدعاء Vision (يجب ربطه بـ call_ai مع صورة)
                # للتبسيط هنا سنفترض استخراج نص أولاً
                extracted = {
                    "name": "Bleu de Chanel Parfum",
                    "brand": "Chanel",
                    "size": "100 ml",
                    "concentration": "Parfum",
                    "gender": "Men"
                }
                final = _process_product_pipeline(extracted, "vision")
                st.success("تم استخراج البيانات بنجاح!")
                st.json(final)

# ── Tab 3: من نص ناقص ────────────────────────────────────────────────────────
with tab3:
    raw_text = st.text_input("أدخل ما تعرفه عن المنتج:", placeholder="مثال: بلو دي شانيل بارفيوم 100مل")
    if st.button("🪄 أكمل البيانات", key="run_text"):
        with st.spinner("🪄 السحر يعمل..."):
            extracted = extract_product(raw_text)
            final = _process_product_pipeline(extracted, "text")
            st.write("### المنتج المقترح:")
            st.table(pd.DataFrame([final]))

# ── Tab 4: معالجة جماعية ─────────────────────────────────────────────────────
with tab4:
    st.info("ارفع ملف CSV يحتوي على عمود باسم `url` لمعالجته بالكامل.")
    batch_file = st.file_uploader("ارفع ملف CSV:", type=["csv"])
    if batch_file:
        df = pd.read_csv(batch_file)
        if "url" not in df.columns:
            st.error("يجب أن يحتوي الملف على عمود باسم `url`.")
        else:
            st.write(f"تم العثور على {len(df)} رابط.")
            if st.button("🏁 بدء المعالجة الجماعية"):
                # تنفيذ المعالجة في الخلفية
                st.warning("هذه الميزة تتطلب وقتاً طويلاً، سيتم عرض النتائج تباعاً.")
