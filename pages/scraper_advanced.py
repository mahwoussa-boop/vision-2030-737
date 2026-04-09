"""
pages/scraper_advanced.py — صفحة الكشط المتقدمة (الوحدة الثانية)
══════════════════════════════════════════════════════════════════
✅ أزرار تحكم فردية لكل منافس (▶️ كشط / 🔄 تحديث / ⏭️ تخطي)
✅ عرض نقاط الاستئناف الحالية لكل متجر
✅ شريط تقدم حي لكل عملية مفردة
✅ بطاقة حالة مرئية لكل منافس (Done / Error / Pending / Running)
"""
import json
import os
import subprocess
import sys
import threading
import time
from datetime import datetime
from urllib.parse import urlparse

import pandas as pd
import streamlit as st

# ── إعداد الصفحة ──────────────────────────────────────────────────────────
st.set_page_config(
    page_title="كاشط مهووس — تحكم متقدم",
    page_icon="🕷️",
    layout="wide",
)

# ── CSS مخصص ──────────────────────────────────────────────────────────────
st.markdown("""
<style>
body, .stApp { direction: rtl; }
.comp-card {
    background: #0d1b2a;
    border: 1px solid #1e3a5f;
    border-radius: 10px;
    padding: 14px 16px;
    margin-bottom: 10px;
    position: relative;
}
.comp-card.done  { border-color: #00C853; }
.comp-card.error { border-color: #FF1744; }
.comp-card.running { border-color: #4fc3f7; animation: pulse 1.5s infinite; }
.comp-card.pending { border-color: #555; }
@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.6} }
.status-badge {
    display: inline-block; padding: 2px 10px;
    border-radius: 12px; font-size: .75rem; font-weight: 700;
}
.badge-done    { background:#00C853; color:#000; }
.badge-error   { background:#FF1744; color:#fff; }
.badge-running { background:#4fc3f7; color:#000; }
.badge-pending { background:#555;    color:#fff; }
.checkpoint-info {
    font-size:.72rem; color:#aaa;
    margin-top:4px;
    display:flex; gap:12px; flex-wrap:wrap;
}
</style>
""", unsafe_allow_html=True)

# ── ثوابت ─────────────────────────────────────────────────────────────────
_DATA_DIR         = os.environ.get("DATA_DIR", "data")
_COMPETITORS_FILE = os.path.join(_DATA_DIR, "competitors_list.json")
_PROGRESS_FILE    = os.path.join(_DATA_DIR, "scraper_progress.json")
_STATE_FILE       = os.path.join(_DATA_DIR, "scraper_state.json")
_OUTPUT_CSV       = os.path.join(_DATA_DIR, "competitors_latest.csv")
_SCRAPER_SCRIPT   = os.path.join("scrapers", "async_scraper.py")


# ══════════════════════════════════════════════════════════════════════════════
#  دوال مساعدة
# ══════════════════════════════════════════════════════════════════════════════

def _domain(url: str) -> str:
    return urlparse(url).netloc.replace("www.", "")


def _load_stores() -> list:
    try:
        return json.loads(open(_COMPETITORS_FILE, encoding="utf-8").read())
    except Exception:
        return []


def _save_stores(lst: list) -> None:
    os.makedirs(_DATA_DIR, exist_ok=True)
    open(_COMPETITORS_FILE, "w", encoding="utf-8").write(
        json.dumps(lst, ensure_ascii=False, indent=2)
    )


def _load_progress() -> dict:
    try:
        return json.loads(open(_PROGRESS_FILE, encoding="utf-8").read())
    except Exception:
        return {"running": False}


def _load_state() -> dict:
    """يُحمّل scraper_state.json — نقاط الاستئناف لكل متجر"""
    try:
        return json.loads(open(_STATE_FILE, encoding="utf-8").read())
    except Exception:
        return {}


def _get_store_checkpoint(domain: str) -> dict:
    state = _load_state()
    return state.get(domain, {})


def _csv_row_count_by_store(domain: str) -> int:
    """يحسب عدد منتجات متجر معين في CSV الرئيسي"""
    try:
        df = pd.read_csv(_OUTPUT_CSV, encoding="utf-8-sig", low_memory=False)
        return int((df["store"].astype(str) == domain).sum())
    except Exception:
        return 0


def _reset_store_state(domain: str) -> None:
    """يمسح نقطة استئناف متجر واحد"""
    state = _load_state()
    if domain in state:
        state[domain]["status"] = "pending"
        state[domain]["last_url_index"] = 0
        state[domain]["last_page"] = 0
        state[domain]["error"] = ""
        with open(_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)


# ══════════════════════════════════════════════════════════════════════════════
#  تشغيل كشط مفرد في خيط منفصل
# ══════════════════════════════════════════════════════════════════════════════

def _run_single_store_bg(
    store_url: str,
    concurrency: int = 8,
    max_products: int = 0,
    force: bool = False,
) -> None:
    """يُشغَّل في خيط منفصل — لا يحجب واجهة Streamlit"""
    try:
        from engines.async_scraper import run_single_store
        result = run_single_store(
            store_url,
            concurrency=concurrency,
            max_products=max_products,
            force=force,
        )
        # تخزين النتيجة في ملف مؤقت ليتمكن Streamlit من قراءتها
        _write_single_result(_domain(store_url), result)
    except Exception as e:
        _write_single_result(_domain(store_url), {
            "success": False, "rows": 0,
            "message": str(e), "domain": _domain(store_url)
        })


def _write_single_result(domain: str, result: dict) -> None:
    path = os.path.join(_DATA_DIR, f"_sc_result_{domain}.json")
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False)
    except Exception:
        pass


def _read_single_result(domain: str) -> dict | None:
    path = os.path.join(_DATA_DIR, f"_sc_result_{domain}.json")
    try:
        return json.loads(open(path, encoding="utf-8").read())
    except Exception:
        return None


def _is_store_running(domain: str) -> bool:
    """يتحقق من نقطة الاستئناف إذا كان المتجر قيد الكشط الآن"""
    cp = _get_store_checkpoint(domain)
    return cp.get("status") == "running"


# ══════════════════════════════════════════════════════════════════════════════
#  بطاقة منافس واحد
# ══════════════════════════════════════════════════════════════════════════════

def render_competitor_card(
    store_url: str,
    idx: int,
    concurrency: int,
    max_products: int,
) -> None:
    """
    يرسم بطاقة كاملة لمنافس واحد مع:
    - عرض الحالة (Done/Error/Running/Pending)
    - نقطة الاستئناف الأخيرة
    - أزرار: [▶️ بدء/تحديث] [🔄 إعادة من البداية] [🗑️ حذف]
    """
    domain = _domain(store_url)
    cp     = _get_store_checkpoint(domain)
    status = cp.get("status", "pending")
    prog   = _load_progress()
    is_global_running = bool(prog.get("running", False))

    # قراءة نتيجة سابقة إن وُجدت
    last_result = _read_single_result(domain)

    # — اختيار لون البطاقة —
    card_cls   = {"done": "done", "error": "error", "running": "running"}.get(status, "pending")
    badge_cls  = {"done": "badge-done", "error": "badge-error",
                  "running": "badge-running"}.get(status, "badge-pending")
    status_ar  = {
        "done":    "✅ مكتمل",
        "error":   "❌ خطأ",
        "running": "⏳ جاري",
        "pending": "⏸️ معلق",
    }.get(status, "⏸️ معلق")

    rows_in_state = int(cp.get("rows_saved", 0))
    rows_in_csv   = _csv_row_count_by_store(domain) if status == "done" else rows_in_state
    urls_done     = int(cp.get("urls_done", 0))
    urls_total    = int(cp.get("urls_total", 0) or 1)
    last_cp_at    = str(cp.get("last_checkpoint_at", "")[:16])
    finished_at   = str(cp.get("finished_at", "")[:16])
    err_msg       = str(cp.get("error", ""))

    # — رسم البطاقة —
    short_url = store_url.replace("https://", "").replace("http://", "").rstrip("/")

    st.markdown(
        f'<div class="comp-card {card_cls}">'
        f'<div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:6px">'
        f'<div>'
        f'<span style="font-weight:700;font-size:.95rem">{idx+1}. {domain}</span>'
        f'&nbsp; <span class="status-badge {badge_cls}">{status_ar}</span>'
        f'</div>'
        f'<div style="font-size:.75rem;color:#888">'
        f'<a href="{store_url}" target="_blank" style="color:#4fc3f7">{short_url}</a>'
        f'</div>'
        f'</div>'
        f'<div class="checkpoint-info">'
        + (f'<span>📦 {rows_in_csv:,} منتج</span>' if rows_in_csv > 0 else '')
        + (f'<span>🔗 {urls_done}/{urls_total} رابط</span>' if urls_total > 1 else '')
        + (f'<span>💾 نقطة: {last_cp_at}</span>' if last_cp_at else '')
        + (f'<span>🏁 اكتمل: {finished_at}</span>' if finished_at else '')
        + (f'<span style="color:#FF7043">⚠️ {err_msg[:60]}</span>' if err_msg else '')
        + f'</div>'
        + f'</div>',
        unsafe_allow_html=True,
    )

    # — الأزرار —
    btn_col1, btn_col2, btn_col3, btn_col4 = st.columns([2, 2, 2, 1])

    # ▶️ بدء / تحديث
    with btn_col1:
        _is_running_now = status == "running"
        btn_label  = "⏳ جاري…" if _is_running_now else (
            "🔄 تحديث (استئناف)" if status == "done" else "▶️ بدء الكشط"
        )
        if st.button(
            btn_label, key=f"sc_start_{idx}",
            disabled=_is_running_now or is_global_running,
            use_container_width=True,
            type="primary" if status != "done" else "secondary",
        ):
            st.session_state[f"_sc_single_started_{domain}"] = True
            t = threading.Thread(
                target=_run_single_store_bg,
                args=(store_url, concurrency, max_products, False),
                daemon=True,
            )
            t.start()
            # تحديث نقطة الاستئناف إلى "running" فوراً
            _update_cp_status(domain, "running")
            st.rerun()

    # 🔄 إعادة من البداية (force)
    with btn_col2:
        if st.button(
            "🔁 من البداية", key=f"sc_force_{idx}",
            disabled=status == "running" or is_global_running,
            use_container_width=True,
        ):
            _reset_store_state(domain)
            t = threading.Thread(
                target=_run_single_store_bg,
                args=(store_url, concurrency, max_products, True),
                daemon=True,
            )
            t.start()
            _update_cp_status(domain, "running")
            st.session_state[f"_sc_msg"] = ("info", f"🔁 إعادة كشط {domain} من الصفر")
            st.rerun()

    # ⏭️ تخطي (تجاهل هذا المتجر)
    with btn_col3:
        if st.button(
            "⏭️ تخطي", key=f"sc_skip_{idx}",
            disabled=status == "running",
            use_container_width=True,
        ):
            _mark_cp_skipped(domain, store_url)
            st.rerun()

    # 🗑️ حذف من القائمة
    with btn_col4:
        if st.button("🗑️", key=f"sc_del_{idx}", help=f"حذف {domain}"):
            stores = _load_stores()
            if store_url in stores:
                stores.remove(store_url)
                _save_stores(stores)
                st.session_state["_sc_msg"] = ("success", f"تم حذف {domain}")
                st.rerun()

    # — عرض نتيجة الكشط الأخيرة —
    if last_result and status != "running":
        if last_result.get("success"):
            st.success(last_result["message"])
        else:
            st.error(f"❌ {last_result['message']}")


def _update_cp_status(domain: str, new_status: str) -> None:
    state = _load_state()
    if domain not in state:
        state[domain] = {"domain": domain, "store_url": "", "status": new_status}
    else:
        state[domain]["status"] = new_status
    try:
        with open(_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def _mark_cp_skipped(domain: str, store_url: str) -> None:
    state = _load_state()
    state[domain] = {
        "domain": domain, "store_url": store_url,
        "status": "done", "rows_saved": 0,
        "last_checkpoint_at": datetime.now().isoformat(),
        "finished_at": datetime.now().isoformat(),
        "error": "skipped",
    }
    with open(_STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


# ══════════════════════════════════════════════════════════════════════════════
#  الصفحة الرئيسية
# ══════════════════════════════════════════════════════════════════════════════

st.title("🕷️ كاشط المنافسين — تحكم متقدم")
st.caption("كشط مستقل لكل منافس مع استئناف ذكي من نقطة التوقف")

# — رسائل النظام —
if msg := st.session_state.pop("_sc_msg", None):
    getattr(st, msg[0])(msg[1])

# ── إعدادات عامة (Sidebar) ────────────────────────────────────────────────
with st.sidebar:
    st.subheader("⚙️ إعدادات الكشط")
    concurrency = st.number_input("طلبات متزامنة", 2, 30, 8, step=1, key="adv_concurrency")
    all_flag    = st.checkbox("جميع المنتجات (بلا سقف)", value=True, key="adv_all")
    max_prod    = st.number_input(
        "أقصى منتجات / متجر", 0, 50000,
        0 if all_flag else 1000, step=500,
        disabled=all_flag, key="adv_max",
    )
    max_products = 0 if all_flag else max_prod

    st.divider()
    st.subheader("🔄 استئناف ذكي")
    state_data = _load_state()
    done_count = sum(1 for c in state_data.values() if c.get("status") == "done")
    err_count  = sum(1 for c in state_data.values() if c.get("status") == "error")
    st.metric("✅ مكتمل", done_count)
    st.metric("❌ أخطاء", err_count)
    st.metric("📋 متاجر مسجلة", len(state_data))

    if st.button("🗑️ مسح كل نقاط الاستئناف", use_container_width=True):
        try:
            open(_STATE_FILE, "w", encoding="utf-8").write("{}")
            st.session_state["_sc_msg"] = ("success", "تم مسح نقاط الاستئناف")
        except Exception as e:
            st.session_state["_sc_msg"] = ("error", str(e))
        st.rerun()

    st.divider()

    # تشغيل الكل
    prog_now   = _load_progress()
    is_running = bool(prog_now.get("running", False))
    if st.button(
        "⏳ الكشط يعمل…" if is_running else "🚀 كشط جميع المتاجر",
        disabled=is_running,
        type="primary",
        use_container_width=True,
        key="adv_run_all",
    ):
        try:
            subprocess.Popen(
                [sys.executable, _SCRAPER_SCRIPT,
                 "--max-products", str(max_products),
                 "--concurrency", str(int(concurrency))],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
            st.session_state["_sc_msg"] = ("success", "🚀 الكشط الكلي بدأ في الخلفية")
        except Exception as e:
            st.session_state["_sc_msg"] = ("error", str(e))
        st.rerun()

# ── لوحة المراقبة العامة ─────────────────────────────────────────────────
prog = _load_progress()
if prog.get("running"):
    try:
        from streamlit_autorefresh import st_autorefresh
        st_autorefresh(interval=3000, key="adv_autorefresh")
    except ImportError:
        pass
    st.info(
        f"🔄 **الكشط الكلي يعمل** — "
        f"{prog.get('current_store','…')} | "
        f"متاجر: {prog.get('stores_done',0)}/{prog.get('stores_total',1)}"
    )

# ── إضافة متجر جديد ──────────────────────────────────────────────────────
st.subheader("➕ إضافة متجر منافس")
c1, c2 = st.columns([5, 1])
new_url = c1.text_input("رابط المتجر", placeholder="https://example.com",
                         label_visibility="collapsed", key="adv_new_url")
if c2.button("➕ إضافة", use_container_width=True, key="adv_add_store"):
    url = (new_url or "").strip()
    if url:
        if not url.startswith("http"):
            url = "https://" + url
        stores = _load_stores()
        if url not in stores:
            stores.append(url)
            _save_stores(stores)
            st.session_state["_sc_msg"] = ("success", f"✅ أُضيف: {url}")
        else:
            st.session_state["_sc_msg"] = ("warning", "الرابط موجود مسبقاً")
        st.rerun()

st.divider()

# ── قائمة المنافسين مع أزرار التحكم الفردية ─────────────────────────────
stores_list = _load_stores()

if not stores_list:
    st.info("لا توجد متاجر — أضف رابطاً للبدء")
else:
    st.subheader(f"🏪 {len(stores_list)} متجر مستهدف")

    # فلتر سريع
    filter_status = st.selectbox(
        "فلتر الحالة",
        ["الكل", "✅ مكتمل", "❌ خطأ", "⏳ جاري", "⏸️ معلق"],
        key="adv_filter_status",
    )
    status_map = {
        "✅ مكتمل": "done", "❌ خطأ": "error",
        "⏳ جاري": "running", "⏸️ معلق": "pending"
    }

    for i, surl in enumerate(stores_list):
        d  = _domain(surl)
        cp = _get_store_checkpoint(d)
        current_status = cp.get("status", "pending")

        if filter_status != "الكل":
            wanted = status_map.get(filter_status, "")
            if current_status != wanted:
                continue

        render_competitor_card(surl, i, int(concurrency), max_products)

# ── لوحة ملخص Checkpoints ────────────────────────────────────────────────
if state_data:
    st.divider()
    with st.expander("📋 تفاصيل نقاط الاستئناف (scraper_state.json)", expanded=False):
        rows = []
        for d, cp in state_data.items():
            rows.append({
                "المتجر":        d,
                "الحالة":        cp.get("status", "pending"),
                "منتجات":        cp.get("rows_saved", 0),
                "روابط (تمت)":   cp.get("urls_done", 0),
                "روابط (كلي)":   cp.get("urls_total", 0),
                "آخر نقطة":      str(cp.get("last_checkpoint_at", ""))[:16],
                "اكتمل":         str(cp.get("finished_at", ""))[:16],
                "خطأ":           str(cp.get("error", ""))[:40],
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True)
