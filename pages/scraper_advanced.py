"""
pages/scraper_advanced.py — لوحة تحكم الكشط المتقدمة v3.0
═══════════════════════════════════════════════════════════════
✅ لوحة تحكم Dashboard احترافية لكل منافس
✅ شريط تقدم حي (Live Progress) بدون تجميد الواجهة
✅ أزرار فردية: ▶️ بدء | 🔁 إعادة | ⏭️ تخطي | 🗑️ حذف
✅ حماية Race Conditions عبر قفل مزامنة Thread-Safe
✅ دمج curl_cffi + cloudscraper + fallback سلسة
✅ Auto-Refresh ذكي — يعمل فقط عند وجود كشط نشط
✅ إدارة session_state صحيحة — لا rerun loops
✅ Checkpoints استئناف محفوظة بالكامل
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import streamlit as st

if __name__ == "__main__":
    st.set_page_config(
        page_title="كاشط مهووس — تحكم متقدم",
        page_icon="🕷️",
        layout="wide",
    )

# ══════════════════════════════════════════════════════════════════════════════
#  CSS الاحترافي
# ══════════════════════════════════════════════════════════════════════════════
_PAGE_STYLES = """
<style>
/* لا تُضبط direction على body/.stApp — يعكس ترتيب الشريط الجانبي/المحتوى في Streamlit */
.comp-card {
    background: linear-gradient(135deg,#0d1b2a,#0a1520);
    border:1.5px solid #1e3a5f; border-radius:12px;
    padding:16px 18px 12px; margin-bottom:12px;
    transition:border-color .3s,box-shadow .3s;
}
.comp-card:hover { box-shadow:0 4px 20px rgba(79,195,247,.15); }
.comp-card.done    { border-color:#00C853; }
.comp-card.error   { border-color:#FF1744; }
.comp-card.skipped { border-color:#FFA000; }
.comp-card.running {
    border-color:#4fc3f7;
    animation:cardPulse 2s ease-in-out infinite;
}
.comp-card.pending { border-color:#37474f; }
@keyframes cardPulse {
    0%,100%{border-color:#4fc3f7;box-shadow:0 0 0 rgba(79,195,247,0)}
    50%{border-color:#81d4fa;box-shadow:0 0 12px rgba(79,195,247,.35)}
}
.status-badge{
    display:inline-flex;align-items:center;gap:4px;
    padding:3px 12px;border-radius:20px;
    font-size:.72rem;font-weight:700;letter-spacing:.3px;
}
.badge-done    {background:rgba(0,200,83,.15);color:#00C853;border:1px solid #00C853;}
.badge-error   {background:rgba(255,23,68,.15);color:#FF1744;border:1px solid #FF1744;}
.badge-running {background:rgba(79,195,247,.2);color:#4fc3f7;border:1px solid #4fc3f7;}
.badge-pending {background:rgba(96,125,139,.15);color:#90a4ae;border:1px solid #37474f;}
.badge-skipped {background:rgba(255,160,0,.15);color:#FFA000;border:1px solid #FFA000;}
.store-meta{
    font-size:.75rem;color:#78909c;
    display:flex;gap:14px;flex-wrap:wrap;margin-top:6px;
}
.live-bar-wrap{margin-top:8px;background:#0a1520;border-radius:6px;height:8px;overflow:hidden;}
.live-bar-fill{
    height:100%;
    background:linear-gradient(90deg,#4fc3f7,#0091ea);
    border-radius:6px;transition:width .5s ease;
}
.stats-row{display:flex;gap:14px;flex-wrap:wrap;margin-bottom:16px;}
.stat-chip{
    background:#0d1b2a;border:1px solid #1e3a5f;
    border-radius:8px;padding:8px 14px;text-align:center;min-width:80px;
}
.stat-chip .sv{font-size:1.4rem;font-weight:700;color:#4fc3f7;}
.stat-chip .sk{font-size:.68rem;color:#78909c;margin-top:2px;}
.store-header{display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px;margin-bottom:4px;}
.store-name{font-weight:700;font-size:1rem;color:#e0e0e0;}
</style>
"""


def _inject_page_styles() -> None:
    st.markdown(_PAGE_STYLES, unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
#  ثوابت ومسارات
# ══════════════════════════════════════════════════════════════════════════════
_DATA_DIR         = os.environ.get("DATA_DIR", "data")
_COMPETITORS_FILE = os.path.join(_DATA_DIR, "competitors_list.json")
_PROGRESS_FILE    = os.path.join(_DATA_DIR, "scraper_progress.json")
_STATE_FILE       = os.path.join(_DATA_DIR, "scraper_state.json")
_OUTPUT_CSV       = os.path.join(_DATA_DIR, "competitors_latest.csv")
_SCRAPER_SCRIPT   = os.path.join("scrapers", "async_scraper.py")

os.makedirs(_DATA_DIR, exist_ok=True)

# أقفال مزامنة لحماية الملفات من Race Conditions
_STATE_LOCK    = threading.Lock()
_RESULT_LOCK   = threading.Lock()
_PROGRESS_LOCK = threading.Lock()


# ══════════════════════════════════════════════════════════════════════════════
#  دوال I/O آمنة للخيوط المتعددة
# ══════════════════════════════════════════════════════════════════════════════

def _domain(url: str) -> str:
    return urlparse(url).netloc.replace("www.", "")


def _load_stores() -> list:
    try:
        with open(_COMPETITORS_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []


def _save_stores(lst: list) -> None:
    os.makedirs(_DATA_DIR, exist_ok=True)
    with _STATE_LOCK:
        with open(_COMPETITORS_FILE, "w", encoding="utf-8") as f:
            json.dump(lst, f, ensure_ascii=False, indent=2)


def _load_progress() -> dict:
    try:
        with _PROGRESS_LOCK:
            with open(_PROGRESS_FILE, encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        return {"running": False}


def _load_state() -> dict:
    try:
        with _STATE_LOCK:
            with open(_STATE_FILE, encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        return {}


def _save_state(state: dict) -> None:
    with _STATE_LOCK:
        with open(_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)


def _get_store_checkpoint(domain: str) -> dict:
    return _load_state().get(domain, {})


def _csv_row_count_by_store(domain: str) -> int:
    try:
        df = pd.read_csv(_OUTPUT_CSV, encoding="utf-8-sig", low_memory=False)
        return int((df["store"].astype(str) == domain).sum())
    except Exception:
        return 0


def _reset_store_state(domain: str) -> None:
    state = _load_state()
    if domain in state:
        state[domain].update({
            "status": "pending",
            "last_url_index": 0,
            "last_page": 0,
            "urls_done": 0,
            "error": "",
            "finished_at": "",
        })
        _save_state(state)


def _update_cp_status(domain: str, new_status: str) -> None:
    state = _load_state()
    if domain not in state:
        state[domain] = {"domain": domain, "store_url": "", "status": new_status}
    else:
        state[domain]["status"] = new_status
    _save_state(state)


def _mark_cp_skipped(domain: str, store_url: str) -> None:
    state = _load_state()
    state[domain] = {
        "domain": domain, "store_url": store_url,
        "status": "done", "rows_saved": 0,
        "last_checkpoint_at": datetime.now().isoformat(),
        "finished_at": datetime.now().isoformat(),
        "error": "skipped",
    }
    _save_state(state)


# ─── ملفات النتيجة الفردية ─────────────────────────────────────────────────

def _result_path(domain: str) -> str:
    return os.path.join(_DATA_DIR, f"_sc_result_{domain}.json")


def _write_single_result(domain: str, result: dict) -> None:
    with _RESULT_LOCK:
        try:
            with open(_result_path(domain), "w", encoding="utf-8") as f:
                json.dump(result, f, ensure_ascii=False)
        except Exception:
            pass


def _read_single_result(domain: str) -> dict | None:
    try:
        with _RESULT_LOCK:
            with open(_result_path(domain), encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        return None


# ─── ملفات تقدم حي per-store ──────────────────────────────────────────────

def _live_progress_path(domain: str) -> str:
    return os.path.join(_DATA_DIR, f"_sc_live_{domain}.json")


def _read_live_progress(domain: str) -> dict:
    try:
        with open(_live_progress_path(domain), encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


# ══════════════════════════════════════════════════════════════════════════════
#  تشغيل كشط مفرد في خيط daemon منفصل
# ══════════════════════════════════════════════════════════════════════════════

def _run_single_store_bg(
    store_url: str,
    concurrency: int = 8,
    max_products: int = 0,
    force: bool = False,
) -> None:
    """
    يُشغَّل في خيط daemon منفصل — يدمج:
    • curl_cffi    → TLS fingerprint حقيقي (يتخطى Cloudflare/Akamai)
    • cloudscraper → JS Challenge fallback
    • aiohttp + anti_ban headers → طلبات عادية مع Rate Limiting
    النتيجة تُكتب في _sc_result_{domain}.json للقراءة من Streamlit
    """
    dom = _domain(store_url)
    try:
        from engines.async_scraper import run_single_store
        result = run_single_store(
            store_url,
            concurrency=concurrency,
            max_products=max_products,
            force=force,
        )
        _write_single_result(dom, result)
    except Exception as exc:
        _write_single_result(dom, {
            "success": False, "rows": 0,
            "message": str(exc), "domain": dom,
        })
    finally:
        # تنظيف ملف التقدم الحي عند الانتهاء
        lp = _live_progress_path(dom)
        try:
            if os.path.exists(lp):
                os.remove(lp)
        except Exception:
            pass


def _start_scrape_thread(
    store_url: str,
    concurrency: int,
    max_products: int,
    force: bool = False,
) -> None:
    """يُحدّث الحالة إلى running ثم يُنشئ الخيط."""
    dom = _domain(store_url)
    _update_cp_status(dom, "running")
    t = threading.Thread(
        target=_run_single_store_bg,
        args=(store_url, concurrency, max_products, force),
        daemon=True,
        name=f"scrape-{dom}",
    )
    t.start()


# ══════════════════════════════════════════════════════════════════════════════
#  بطاقة المنافس — Dashboard Card
# ══════════════════════════════════════════════════════════════════════════════

def _badge_html(status: str) -> str:
    labels = {
        "done":    ("badge-done",    "✅ مكتمل"),
        "error":   ("badge-error",   "❌ خطأ"),
        "running": ("badge-running", "⏳ جاري"),
        "skipped": ("badge-skipped", "⏭️ متخطى"),
        "pending": ("badge-pending", "⏸️ معلق"),
    }
    cls, text = labels.get(status, ("badge-pending", "⏸️ معلق"))
    return f'<span class="status-badge {cls}">{text}</span>'


def render_competitor_card(
    store_url: str,
    idx: int,
    concurrency: int,
    max_products: int,
    is_global_running: bool,
) -> None:
    dom    = _domain(store_url)
    cp     = _get_store_checkpoint(dom)
    status = cp.get("status", "pending")
    if status == "done" and cp.get("error") == "skipped":
        status = "skipped"

    is_running_now = (status == "running")

    urls_done  = int(cp.get("urls_done", 0))
    urls_total = int(cp.get("urls_total", 0) or 1)
    rows_saved = int(cp.get("rows_saved", 0))
    last_cp    = str(cp.get("last_checkpoint_at", ""))[:16]
    finished   = str(cp.get("finished_at", ""))[:16]
    err_msg    = str(cp.get("error", ""))
    if err_msg == "skipped":
        err_msg = ""

    # دمج التقدم الحي للمتاجر الجارية
    if is_running_now:
        live = _read_live_progress(dom)
        if live:
            urls_done  = live.get("urls_done", urls_done)
            urls_total = live.get("urls_total", urls_total) or 1
            rows_saved = live.get("rows_saved", rows_saved)

    pct      = min(100, int(urls_done / max(urls_total, 1) * 100))
    card_cls = {"done":"done","error":"error","running":"running","skipped":"skipped"}.get(status,"pending")
    short    = store_url.replace("https://","").replace("http://","").rstrip("/")

    # ── رأس البطاقة ────────────────────────────────────────────────────
    bar_html = (
        f'<div class="live-bar-wrap">'
        f'<div class="live-bar-fill" style="width:{pct}%"></div></div>'
        f'<div style="font-size:.68rem;color:#4fc3f7;text-align:left;margin-top:2px">'
        f'{pct}% &nbsp;({urls_done:,}/{urls_total:,} رابط — {rows_saved:,} منتج)</div>'
    ) if (is_running_now or (status == "done" and urls_total > 1 and not err_msg)) else ""

    st.markdown(
        f'<div class="comp-card {card_cls}">'
        f'<div class="store-header">'
        f'<span class="store-name">#{idx+1} &nbsp;{dom}</span>'
        f'&nbsp;{_badge_html(status)}'
        f'</div>'
        f'<div class="store-meta">'
        + (f'<span>📦 {rows_saved:,} منتج</span>'        if rows_saved else "")
        + (f'<span>🔗 {urls_done:,}/{urls_total:,}</span>' if urls_total > 1 else "")
        + (f'<span>💾 نقطة: {last_cp}</span>'             if last_cp    else "")
        + (f'<span>🏁 {finished}</span>'                  if finished   else "")
        + (f'<span style="color:#FF7043">⚠️ {err_msg[:70]}</span>' if err_msg else "")
        + f'</div>'
        + bar_html
        + '</div>',
        unsafe_allow_html=True,
    )

    # ── أزرار التحكم ───────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns([2.5, 2.5, 2.5, 1])

    with c1:
        if is_running_now:
            lbl = "⏳ جاري…"
        elif status == "done":
            lbl = "🔄 تحديث (استئناف)"
        elif status == "skipped":
            lbl = "▶️ كشط الآن"
        else:
            lbl = "▶️ بدء الكشط"

        if st.button(
            lbl, key=f"sc_start_{idx}",
            disabled=is_running_now or is_global_running,
            use_container_width=True,
            type="primary" if status not in ("done","skipped") else "secondary",
        ):
            _start_scrape_thread(store_url, concurrency, max_products, force=False)
            st.session_state["_sc_msg"] = ("info", f"▶️ بدأ كشط {dom}")
            st.rerun()

    with c2:
        if st.button(
            "🔁 من الصفر", key=f"sc_force_{idx}",
            disabled=is_running_now or is_global_running,
            use_container_width=True,
        ):
            _reset_store_state(dom)
            _start_scrape_thread(store_url, concurrency, max_products, force=True)
            st.session_state["_sc_msg"] = ("info", f"🔁 إعادة كشط {dom} من الصفر")
            st.rerun()

    with c3:
        if st.button(
            "⏭️ تخطي", key=f"sc_skip_{idx}",
            disabled=is_running_now,
            use_container_width=True,
        ):
            _mark_cp_skipped(dom, store_url)
            st.session_state["_sc_msg"] = ("warning", f"⏭️ تم تخطي {dom}")
            st.rerun()

    with c4:
        if st.button("🗑️", key=f"sc_del_{idx}", help=f"حذف {dom}"):
            stores = _load_stores()
            if store_url in stores:
                stores.remove(store_url)
                _save_stores(stores)
            st.session_state["_sc_msg"] = ("success", f"🗑️ تم حذف {dom}")
            st.rerun()

    # ── آخر نتيجة ─────────────────────────────────────────────────────
    last_res = _read_single_result(dom)
    if last_res and not is_running_now:
        if last_res.get("success"):
            st.success(last_res["message"], icon="✅")
        else:
            st.error(f"❌ {last_res.get('message','خطأ')}", icon="🚨")


# ══════════════════════════════════════════════════════════════════════════════
#  شريط الإحصائيات العلوي
# ══════════════════════════════════════════════════════════════════════════════

def _render_stats_bar(state_data: dict, stores_count: int) -> None:
    done    = sum(1 for c in state_data.values() if c.get("status") == "done")
    errors  = sum(1 for c in state_data.values() if c.get("status") == "error")
    running = sum(1 for c in state_data.values() if c.get("status") == "running")
    pending = max(0, stores_count - done - errors - running)
    st.markdown(
        f'<div class="stats-row">'
        f'<div class="stat-chip"><div class="sv">{stores_count}</div><div class="sk">متاجر</div></div>'
        f'<div class="stat-chip" style="border-color:#00C853"><div class="sv" style="color:#00C853">{done}</div><div class="sk">مكتملة</div></div>'
        f'<div class="stat-chip" style="border-color:#4fc3f7"><div class="sv" style="color:#4fc3f7">{running}</div><div class="sk">جارية</div></div>'
        f'<div class="stat-chip" style="border-color:#FF1744"><div class="sv" style="color:#FF1744">{errors}</div><div class="sk">أخطاء</div></div>'
        f'<div class="stat-chip"><div class="sv" style="color:#90a4ae">{pending}</div><div class="sk">معلقة</div></div>'
        f'</div>',
        unsafe_allow_html=True,
    )


# ══════════════════════════════════════════════════════════════════════════════
#  نقطة الدخول الرئيسية
# ══════════════════════════════════════════════════════════════════════════════

def show(*, embedded: bool = False) -> None:
    _inject_page_styles()
    st.title("🕷️ كاشط المنافسين — لوحة التحكم المتقدمة")
    st.caption("كشط مستقل لكل منافس مع استئناف ذكي، تخطي Cloudflare، وتحديث حي")

    # ── رسائل النظام ────────────────────────────────────────────────────
    if msg := st.session_state.pop("_sc_msg", None):
        getattr(st, msg[0])(msg[1])

    # ════════════════════════════════════════════════════════════════════
    #  Sidebar (صفحة مستقلة فقط — التضمين في app.py يفسد ترتيب الشريط الجانبي)
    # ════════════════════════════════════════════════════════════════════
    if embedded:
        # إعدادات الكشط من شاشة «كشط المنافسين» الرئيسية (لا ودجات adv_* في الشريط)
        concurrency = int(st.session_state.get("sc_concurrency", 8) or 8)
        _all_emb = bool(st.session_state.get("sc_all_products", True))
        _max_emb = int(st.session_state.get("sc_max_prod", 0) or 0)
        max_products = 0 if _all_emb else _max_emb
    else:
        with st.sidebar:
            st.subheader("⚙️ إعدادات الكشط")
            concurrency = st.number_input(
                "طلبات متزامنة", 2, 30, 8, step=1, key="adv_concurrency",
                help="عدد الطلبات المتزامنة لكل متجر",
            )
            all_flag = st.checkbox("جميع المنتجات (بلا سقف)", value=True, key="adv_all")
            max_prod = st.number_input(
                "أقصى منتجات / متجر", 0, 50000,
                value=0 if all_flag else 1000, step=500,
                disabled=all_flag, key="adv_max",
            )
            max_products = 0 if all_flag else max_prod

            st.divider()
            st.subheader("📊 ملخص الاستئناف")
            state_data = _load_state()
            ca, cb = st.columns(2)
            ca.metric("✅ مكتمل", sum(1 for c in state_data.values() if c.get("status") == "done"))
            cb.metric("❌ أخطاء", sum(1 for c in state_data.values() if c.get("status") == "error"))
            st.metric("📋 مسجّل", len(state_data))

            if st.button("🗑️ مسح كل نقاط الاستئناف", use_container_width=True):
                _save_state({})
                st.session_state["_sc_msg"] = ("success", "✅ تم مسح جميع نقاط الاستئناف")
                st.rerun()

            st.divider()
            prog_now   = _load_progress()
            is_running = bool(prog_now.get("running", False))

            st.subheader("🚀 الكشط الشامل")
            if is_running:
                st.info(
                    f"🔄 **يعمل الآن**\n"
                    f"المتجر: `{prog_now.get('current_store','…')}`\n"
                    f"متاجر: {prog_now.get('stores_done',0)}/{prog_now.get('stores_total',1)}"
                )
            else:
                if st.button(
                    "🚀 كشط جميع المتاجر",
                    type="primary", use_container_width=True, key="adv_run_all",
                ):
                    try:
                        subprocess.Popen(
                            [sys.executable, _SCRAPER_SCRIPT,
                             "--max-products", str(max_products),
                             "--concurrency", str(int(concurrency))],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                            start_new_session=True,
                        )
                        st.session_state["_sc_msg"] = ("success", "🚀 الكشط الكلي بدأ في الخلفية")
                    except Exception as e:
                        st.session_state["_sc_msg"] = ("error", str(e))
                    st.rerun()

            st.divider()
            with st.expander("🛡️ تقنيات ضد الحظر"):
                st.markdown("""
- 🔐 **curl_cffi** — TLS fingerprint (يتخطى Cloudflare)
- 🕵️ **cloudscraper** — JS Challenge fallback
- 🔄 **User-Agents 2026** حقيقية
- ⏱️ **Rate Limiter تكيّفي** عند 429/403
- 🔁 **Exponential Backoff** مع Jitter
                """)

    # ════════════════════════════════════════════════════════════════════
    #  Auto-Refresh ذكي — فقط عند وجود كشط نشط
    # ════════════════════════════════════════════════════════════════════
    state_data  = _load_state()
    any_running = any(c.get("status") == "running" for c in state_data.values())
    global_run  = bool(_load_progress().get("running", False))

    if any_running or global_run:
        try:
            from streamlit_autorefresh import st_autorefresh
            st_autorefresh(interval=2500, key="adv_autorefresh")
        except ImportError:
            st.caption("💡 `pip install streamlit-autorefresh` للتحديث التلقائي")

    # ════════════════════════════════════════════════════════════════════
    #  إضافة متجر
    # ════════════════════════════════════════════════════════════════════
    st.subheader("➕ إضافة متجر منافس")
    cu, ca2 = st.columns([5, 1])
    new_url = cu.text_input(
        "رابط المتجر", placeholder="https://store.example.com",
        label_visibility="collapsed", key="adv_new_url",
    )
    if ca2.button("➕ إضافة", use_container_width=True, key="adv_add_store"):
        url = (new_url or "").strip()
        if url:
            if not url.startswith("http"):
                url = "https://" + url
            stores = _load_stores()
            if url not in stores:
                stores.append(url)
                _save_stores(stores)
                st.session_state["_sc_msg"] = ("success", f"✅ أُضيف: {_domain(url)}")
            else:
                st.session_state["_sc_msg"] = ("warning", "⚠️ الرابط موجود مسبقاً")
            st.rerun()
        else:
            st.warning("أدخل رابطاً صحيحاً")

    st.divider()

    # ════════════════════════════════════════════════════════════════════
    #  لوحة المنافسين
    # ════════════════════════════════════════════════════════════════════
    stores_list = _load_stores()
    state_data  = _load_state()

    if not stores_list:
        st.info("🏪 لا توجد متاجر — أضف رابطاً للبدء", icon="ℹ️")
        return

    # ─ إحصائيات علوية ───────────────────────────────────────────────
    _render_stats_bar(state_data, len(stores_list))

    # ─ فلتر + بحث ───────────────────────────────────────────────────
    cf1, cf2 = st.columns([2, 3])
    with cf1:
        filter_status = st.selectbox(
            "فلتر", ["الكل","✅ مكتمل","❌ خطأ","⏳ جاري","⏸️ معلق","⏭️ متخطى"],
            key="adv_filter_status", label_visibility="collapsed",
        )
    with cf2:
        search_q = st.text_input(
            "بحث", placeholder="🔍 ابحث بالنطاق أو الرابط…",
            label_visibility="collapsed", key="adv_search",
        ).strip().lower()

    status_map = {
        "✅ مكتمل":"done","❌ خطأ":"error",
        "⏳ جاري":"running","⏸️ معلق":"pending","⏭️ متخطى":"skipped",
    }
    _pri = {"running":0,"error":1,"pending":2,"done":3,"skipped":4}

    def _sort_key(url: str):
        d  = _domain(url)
        cp = state_data.get(d, {})
        s  = cp.get("status", "pending")
        if s == "done" and cp.get("error") == "skipped":
            s = "skipped"
        return (_pri.get(s, 99), d)

    shown = 0
    for i, surl in enumerate(sorted(stores_list, key=_sort_key)):
        d  = _domain(surl)
        cp = state_data.get(d, {})
        sv = cp.get("status", "pending")
        if sv == "done" and cp.get("error") == "skipped":
            sv = "skipped"

        if filter_status != "الكل":
            if sv != status_map.get(filter_status, ""):
                continue
        if search_q and search_q not in surl.lower():
            continue

        render_competitor_card(surl, i, int(concurrency), max_products, global_run)
        shown += 1

    if shown == 0:
        st.info("لا توجد متاجر تطابق الفلتر", icon="🔍")

    # ════════════════════════════════════════════════════════════════════
    #  جدول نقاط الاستئناف التفصيلي
    # ════════════════════════════════════════════════════════════════════
    if state_data:
        st.divider()
        with st.expander("📋 جدول نقاط الاستئناف التفصيلي", expanded=False):
            rows = []
            for d, cp in state_data.items():
                sv = cp.get("status", "pending")
                if sv == "done" and cp.get("error") == "skipped":
                    sv = "skipped"
                rows.append({
                    "المتجر":       d,
                    "الحالة":       sv,
                    "منتجات":       cp.get("rows_saved", 0),
                    "روابط (تمت)": cp.get("urls_done", 0),
                    "روابط (كلي)": cp.get("urls_total", 0),
                    "آخر نقطة":    str(cp.get("last_checkpoint_at", ""))[:16],
                    "اكتمل":       str(cp.get("finished_at", ""))[:16],
                    "خطأ":         str(cp.get("error", ""))[:50],
                })
            df = pd.DataFrame(rows)
            st.dataframe(df, use_container_width=True, hide_index=True)
            csv_b = df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
            st.download_button(
                "⬇️ تصدير CSV", data=csv_b,
                file_name="scraper_checkpoints.csv",
                mime="text/csv", key="adv_export_cp",
            )


if __name__ == "__main__":
    _inject_page_styles()
    show()
