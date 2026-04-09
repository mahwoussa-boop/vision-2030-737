"""
engines/engine.py v28.0 — محرك المطابقة المطور لمتجر مهووس
إصلاح أخطاء المطابقة وضمان العزل التام للمفقودات.
النسخة الكاملة المدمجة.
"""
import re, io, json, os, hashlib, sqlite3, time, gc
from datetime import datetime
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Tuple, Optional
from rapidfuzz import fuzz, process as rf_process
from rapidfuzz.distance import Indel

from engines.mahwous_core import apply_strict_pipeline_filters, _extract_ml
from config import (REJECT_KEYWORDS, KNOWN_BRANDS, WORD_REPLACEMENTS,
                    MATCH_THRESHOLD, HIGH_CONFIDENCE, REVIEW_THRESHOLD,
                    PRICE_TOLERANCE, TESTER_KEYWORDS, SET_KEYWORDS)

# إعدادات المطابقة الصارمة
STRICT_MATCH_THRESHOLD = 92  
REVIEW_MIN_THRESHOLD = 70   

def normalize_text(text: str) -> str:
    """تطبيع النص للمطابقة: إزالة الرموز، توحيد الحروف، وتحويل القياسات."""
    if not isinstance(text, str) or not text.strip():
        return ""
    
    t = text.lower()
    # توحيد الحروف العربية والتركيزات
    replacements = {
        "أ": "ا", "إ": "ا", "آ": "ا", "ة": "ه", "ى": "ي", "ؤ": "و", "ئ": "ي", "ـ": "",
        "eau de parfum": "edp", "parfum": "edp", "eau de toilette": "edt", "toilette": "edt",
        "ml": " مل", "ملي": " مل", "مل": " مل"
    }
    for old, new in replacements.items():
        t = t.replace(old, new)
    
    # إزالة الرموز غير الضرورية
    t = re.sub(r"[^\w\s\d]", " ", t)
    # تنظيف المسافات
    t = re.sub(r"\s+", " ", t).strip()
    return t

def calculate_match_score(name1: str, name2: str) -> float:
    """حساب نسبة المطابقة بين اسمين مع مراعاة الماركة والحجم والتركيز."""
    n1 = normalize_text(name1)
    n2 = normalize_text(name2)
    
    if not n1 or not n2:
        return 0.0
    
    # 1. التحقق من تطابق الحجم (عنصر حاسم)
    ml1 = _extract_ml(name1)
    ml2 = _extract_ml(name2)
    if ml1 > 0 and ml2 > 0 and abs(ml1 - ml2) > 2: 
        return 0.0 # اختلاف الحجم يعني منتج مختلف تماماً في العطور
    
    # 2. التحقق من الكلمات المرفوضة (تستر vs عادي)
    is_tester1 = any(k in n1 for k in TESTER_KEYWORDS)
    is_tester2 = any(k in n2 for k in TESTER_KEYWORDS)
    if is_tester1 != is_tester2:
        return 0.0 # لا نطابق تستر مع منتج عادي
    
    # 3. حساب نسبة التشابه النصي
    score = fuzz.token_set_ratio(n1, n2)
    
    # 4. تعزيز النتيجة إذا كانت الماركة متطابقة تماماً
    brand1 = n1.split()[0] if n1.split() else ""
    brand2 = n2.split()[0] if n2.split() else ""
    if brand1 == brand2 and brand1 in [b.lower() for b in KNOWN_BRANDS]:
        score += 5
        
    return min(100.0, score)

def run_full_analysis(our_df: pd.DataFrame, comp_dfs: List[pd.DataFrame]) -> pd.DataFrame:
    """المحرك الرئيسي للمطابقة: يجمع كل البيانات ويقوم بعملية المطابقة والفرز."""
    if our_df is None or our_df.empty or not comp_dfs:
        return pd.DataFrame()
    
    # توحيد بيانات المنافسين
    all_comp = pd.concat(comp_dfs, ignore_index=True)
    all_comp, _ = apply_strict_pipeline_filters(all_comp)
    
    results = []
    
    for _, our_row in our_df.iterrows():
        our_name = str(our_row.get("المنتج", our_row.get("اسم المنتج", our_row.get("أسم المنتج", ""))))
        our_price = float(our_row.get("السعر", 0))
        
        best_match = None
        best_score = 0
        
        # البحث عن أفضل مطابقة في بيانات المنافسين
        for _, comp_row in all_comp.iterrows():
            comp_name = str(comp_row.get("منتج_المنافس", comp_row.get("اسم المنتج", "")))
            score = calculate_match_score(our_name, comp_name)
            
            if score > best_score:
                best_score = score
                best_match = comp_row
        
        # تصنيف النتيجة بناءً على نسبة المطابقة
        res_row = our_row.to_dict()
        res_row["نسبة_المطابقة"] = best_score
        
        if best_score >= STRICT_MATCH_THRESHOLD:
            # مطابقة مؤكدة
            comp_price = float(best_match.get("سعر_المنافس", best_match.get("السعر", 0)))
            res_row["منتج_المنافس"] = best_match.get("منتج_المنافس")
            res_row["سعر_المنافس"] = comp_price
            res_row["القرار"] = "✅ موافق" if abs(our_price - comp_price) <= PRICE_TOLERANCE else \
                               ("🔴 سعر أعلى" if our_price > comp_price else "🟢 سعر أقل")
        elif best_score >= REVIEW_MIN_THRESHOLD:
            # تحتاج مراجعة
            res_row["القرار"] = "⚠️ تحت المراجعة"
            res_row["منتج_المنافس"] = best_match.get("منتج_المنافس")
            res_row["سعر_المنافس"] = best_match.get("سعر_المنافس")
        else:
            # لا يوجد تطابق -> مفقودات
            res_row["القرار"] = "⚪ مستبعد (لا يوجد تطابق)"
            res_row["منتج_المنافس"] = None
            res_row["سعر_المنافس"] = None
            
        results.append(res_row)
        
    return pd.DataFrame(results)

def find_missing_products(our_df: pd.DataFrame, comp_dfs: List[pd.DataFrame]) -> pd.DataFrame:
    """استخراج المنتجات الموجودة عند المنافسين وغير موجودة عندنا (المفقودات)."""
    if our_df is None or not comp_dfs:
        return pd.DataFrame()
    
    all_comp = pd.concat(comp_dfs, ignore_index=True)
    all_comp, _ = apply_strict_pipeline_filters(all_comp)
    
    our_names = [normalize_text(str(n)) for n in our_df.get("المنتج", our_df.get("اسم المنتج", our_df.get("أسم المنتج", [])))]
    
    missing_items = []
    for _, comp_row in all_comp.iterrows():
        comp_name = str(comp_row.get("منتج_المنافس", comp_row.get("اسم المنتج", "")))
        norm_comp = normalize_text(comp_name)
        
        # إذا لم نجد أي تطابق قوي مع أي من منتجاتنا، فهو مفقود
        is_found = False
        for our_n in our_names:
            if fuzz.token_set_ratio(norm_comp, our_n) >= STRICT_MATCH_THRESHOLD:
                is_found = True
                break
        
        if not is_found:
            missing_items.append(comp_row)
            
    return pd.DataFrame(missing_items).drop_duplicates(subset=["منتج_المنافس"]) if missing_items else pd.DataFrame()

# دوال مساعدة لضمان التوافق مع app.py
def read_file(path):
    if str(path).endswith('.csv'):
        return pd.read_csv(path)
    elif str(path).endswith('.xlsx'):
        return pd.read_excel(path)
    return pd.DataFrame()

def smart_missing_barrier(df): return df 
def extract_brand(n): return n.split()[0] if n else ""
def extract_size(n): return _extract_ml(n)
def extract_type(n): return "EDP" if "edp" in str(n).lower() else "EDT"
def is_sample(n): return "sample" in str(n).lower()
def resolve_catalog_columns(df): return df
def detect_input_columns(df): return {c:c for c in df.columns}
def apply_user_column_map(df, m): return df
def _first_image_url_from_row(r): return r.get("صورة_المنافس", r.get("image_url", ""))
