"""
engines/async_scraper.py — محرك الكشط الرئيسي v2.0 (MASTER)
═══════════════════════════════════════════════════════════════
✅ نقاط استئناف ذكية (Checkpointing) لكل منافس على حدة
✅ استئناف تلقائي من آخر نقطة توقف عند الانقطاع
✅ كشط مفرد لأي منافس بشكل مستقل (run_single_store)
✅ دعم كامل للـ CLI (--store / --resume / --reset-state)
✅ شيمات scrapers/ و utils/ و make/ تستورد منه تلقائياً
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, asdict, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse

import aiohttp
import pandas as pd

# ─── إعداد السجل ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("AsyncScraper")

# ─── مسارات البيانات ──────────────────────────────────────────────────────────
_DATA_DIR = os.environ.get("DATA_DIR", "data")
os.makedirs(_DATA_DIR, exist_ok=True)

COMPETITORS_FILE = os.path.join(_DATA_DIR, "competitors_list.json")
OUTPUT_CSV       = os.path.join(_DATA_DIR, "competitors_latest.csv")
PROGRESS_FILE    = os.path.join(_DATA_DIR, "scraper_progress.json")
LASTMOD_FILE     = os.path.join(_DATA_DIR, "scraper_lastmod.json")
STATE_FILE       = os.path.join(_DATA_DIR, "scraper_state.json")   # نقاط الاستئناف

CSV_COLS = [
    "store", "name", "price", "original_price",
    "sku", "url", "image", "brand", "category",
    "availability", "scraped_at",
]


# ══════════════════════════════════════════════════════════════════════════════
#  هياكل البيانات
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class Progress:
    """تقدم الكشط الكلي — يُكتب دورياً إلى PROGRESS_FILE"""
    running: bool = False
    started_at: str = ""
    finished_at: str = ""
    stores_total: int = 0
    stores_done: int = 0
    urls_total: int = 0
    urls_processed: int = 0
    rows_in_csv: int = 0
    fetch_exceptions: int = 0
    success_rate_pct: float = 0.0
    current_store: str = ""
    store_urls_done: int = 0
    store_urls_total: int = 0
    last_error: str = ""
    stores_results: Dict[str, int] = field(default_factory=dict)

    def save(self, path: str = PROGRESS_FILE) -> None:
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(asdict(self), f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"تعذّر حفظ التقدم: {e}")

    @classmethod
    def load(cls, path: str = PROGRESS_FILE) -> "Progress":
        try:
            with open(path, encoding="utf-8") as f:
                return cls(**json.load(f))
        except Exception:
            return cls()


@dataclass
class StoreCheckpoint:
    """نقطة استئناف خاصة بمتجر واحد"""
    store_url: str
    domain: str
    status: str = "pending"       # pending | running | done | error
    last_page: int = 0            # رقم الصفحة الأخيرة (لـ /products.json)
    last_url_index: int = 0       # فهرس آخر URL في قائمة sitemap
    urls_done: int = 0
    urls_total: int = 0
    rows_saved: int = 0
    started_at: str = ""
    finished_at: str = ""
    error: str = ""
    last_checkpoint_at: str = ""


class ScraperState:
    """
    نظام نقاط الاستئناف الكامل.
    يقرأ/يكتب scraper_state.json ويحتفظ بحالة كل متجر.
    """

    def __init__(self, path: str = STATE_FILE):
        self._path = path
        self._data: Dict[str, StoreCheckpoint] = {}
        self._load()

    # ── قراءة / كتابة ────────────────────────────────────────────────────────

    def _load(self) -> None:
        try:
            with open(self._path, encoding="utf-8") as f:
                raw = json.load(f)
            for domain, d in raw.items():
                try:
                    self._data[domain] = StoreCheckpoint(**d)
                except Exception:
                    pass
        except Exception:
            self._data = {}

    def save(self) -> None:
        try:
            out = {k: asdict(v) for k, v in self._data.items()}
            with open(self._path, "w", encoding="utf-8") as f:
                json.dump(out, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"تعذّر حفظ الحالة: {e}")

    # ── واجهة المستخدم ───────────────────────────────────────────────────────

    def get(self, domain: str, store_url: str) -> StoreCheckpoint:
        if domain not in self._data:
            self._data[domain] = StoreCheckpoint(store_url=store_url, domain=domain)
        return self._data[domain]

    def update(self, domain: str, **kwargs) -> None:
        if domain in self._data:
            cp = self._data[domain]
            for k, v in kwargs.items():
                if hasattr(cp, k):
                    setattr(cp, k, v)
            cp.last_checkpoint_at = datetime.now().isoformat()
            self.save()

    def mark_done(self, domain: str, rows: int) -> None:
        self.update(
            domain,
            status="done",
            rows_saved=rows,
            finished_at=datetime.now().isoformat(),
        )

    def mark_error(self, domain: str, error: str) -> None:
        self.update(domain, status="error", error=error[:200])

    def is_done(self, domain: str) -> bool:
        return self._data.get(domain, StoreCheckpoint("", "")).status == "done"

    def reset(self, domain: str | None = None) -> None:
        """إعادة تعيين متجر واحد أو الكل"""
        if domain:
            if domain in self._data:
                cp = self._data[domain]
                cp.status = "pending"
                cp.last_page = 0
                cp.last_url_index = 0
                cp.urls_done = 0
                cp.error = ""
                self.save()
        else:
            self._data = {}
            self.save()

    def get_summary(self) -> dict:
        total = len(self._data)
        done  = sum(1 for c in self._data.values() if c.status == "done")
        err   = sum(1 for c in self._data.values() if c.status == "error")
        return {"total": total, "done": done, "errors": err, "pending": total - done - err}

    def all_checkpoints(self) -> Dict[str, StoreCheckpoint]:
        return self._data


# ══════════════════════════════════════════════════════════════════════════════
#  استخراج المنتجات من JSON / HTML
# ══════════════════════════════════════════════════════════════════════════════

def _domain(url: str) -> str:
    return urlparse(url).netloc.replace("www.", "")


def extract_product(data: dict, store_url: str) -> dict | None:
    """
    يُحوّل بيانات منتج خام إلى صف موحّد بـ CSV_COLS.
    يدعم: Shopify / Salla / Zid / WooCommerce / HTML meta
    """
    name = (
        data.get("name") or data.get("title") or
        data.get("product_name") or data.get("الاسم") or ""
    ).strip()
    if not name:
        return None

    def _price(raw):
        try:
            return float(str(raw).replace(",", "").replace("ر.س", "").strip())
        except Exception:
            return 0.0

    price = _price(
        data.get("price") or data.get("Price") or
        data.get("regular_price") or data.get("السعر") or 0
    )
    orig  = _price(
        data.get("compare_at_price") or data.get("original_price") or
        data.get("السعر_الأصلي") or price
    )
    sku   = str(data.get("sku") or data.get("id") or data.get("SKU") or "")
    url   = (data.get("url") or data.get("link") or data.get("handle") or "").strip()
    if url and not url.startswith("http"):
        base = store_url.rstrip("/")
        url  = f"{base}/{url.lstrip('/')}"
    image = (
        data.get("image") or data.get("featured_image") or
        data.get("thumbnail") or ""
    )
    if isinstance(image, dict):
        image = image.get("src", "")
    brand = str(data.get("vendor") or data.get("brand") or data.get("الماركة") or "")
    cat   = str(data.get("product_type") or data.get("category") or "")
    avail = str(data.get("available") or data.get("in_stock") or "true")

    return {
        "store":          _domain(store_url),
        "name":           name,
        "price":          price,
        "original_price": orig,
        "sku":            sku,
        "url":            url,
        "image":          image if isinstance(image, str) else "",
        "brand":          brand,
        "category":       cat,
        "availability":   avail,
        "scraped_at":     datetime.now().isoformat()[:19],
    }


# ══════════════════════════════════════════════════════════════════════════════
#  جلب منتج واحد من URL
# ══════════════════════════════════════════════════════════════════════════════

async def fetch_product(
    session: aiohttp.ClientSession,
    url: str,
    store_url: str,
    semaphore: asyncio.Semaphore,
) -> dict | None:
    """
    يجلب صفحة/API منتج ويُعيد dict موحّد أو None.
    الترتيب: Shopify JSON → JSON-LD → og:meta
    """
    async with semaphore:
        # ── Shopify-style .json ──
        json_url = url if url.endswith(".json") else url.rstrip("/") + ".json"
        try:
            async with session.get(
                json_url, timeout=aiohttp.ClientTimeout(total=12), ssl=False
            ) as resp:
                if resp.status == 200 and "json" in resp.headers.get("Content-Type", ""):
                    data = await resp.json(content_type=None)
                    prod = data.get("product", data)
                    row  = extract_product(prod, store_url)
                    if row:
                        return row
        except Exception:
            pass

        # ── HTML fallback ──
        try:
            from scrapers.anti_ban import get_browser_headers
            hdrs = get_browser_headers(store_url)
        except Exception:
            hdrs = {"User-Agent": "Mozilla/5.0 Chrome/120"}

        try:
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=18),
                headers=hdrs, ssl=False, allow_redirects=True,
            ) as resp:
                if resp.status != 200:
                    return None
                html = await resp.text(errors="replace")
        except Exception:
            return None

        import re

        # JSON-LD
        ld_match = re.search(
            r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
            html, re.S | re.I,
        )
        if ld_match:
            try:
                ld = json.loads(ld_match.group(1))
                if isinstance(ld, list):
                    ld = next((x for x in ld if x.get("@type") == "Product"), {})
                if ld.get("@type") == "Product":
                    offer = ld.get("offers", {})
                    if isinstance(offer, list):
                        offer = offer[0] if offer else {}
                    imgs = ld.get("image", "")
                    img  = imgs[0] if isinstance(imgs, list) else imgs
                    row  = extract_product({
                        "name":  ld.get("name", ""),
                        "price": offer.get("price", 0),
                        "image": img,
                        "brand": (ld.get("brand") or {}).get("name", "") if isinstance(ld.get("brand"), dict) else str(ld.get("brand", "")),
                        "url":   url,
                        "sku":   ld.get("sku", ""),
                        "available": offer.get("availability", ""),
                    }, store_url)
                    if row:
                        return row
            except Exception:
                pass

        # og:meta
        def _meta(prop: str) -> str:
            m = re.search(
                rf'<meta[^>]+(?:property|name)=["\']og:{prop}["\'][^>]+content=["\']([^"\']+)["\']',
                html, re.I,
            )
            return m.group(1).strip() if m else ""

        pname = _meta("title")
        if pname:
            return extract_product(
                {"name": pname, "image": _meta("image"), "url": url, "price": 0},
                store_url,
            )

        return None


# ══════════════════════════════════════════════════════════════════════════════
#  كاشط متجر واحد مع نقاط استئناف
# ══════════════════════════════════════════════════════════════════════════════

async def scrape_one_store(
    store_url: str,
    progress: Progress,
    state: ScraperState,
    concurrency: int = 8,
    max_products: int = 0,
    resume: bool = True,
    single_mode: bool = False,
) -> List[dict]:
    """
    يكشط متجراً واحداً مع دعم الاستئناف من نقطة التوقف.

    Args:
        store_url:    رابط جذر المتجر
        progress:     كائن التقدم العام (يُحدَّث مباشرةً)
        state:        كائن نقاط الاستئناف
        concurrency:  عدد الطلبات المتزامنة
        max_products: 0 = بلا حد
        resume:       True → استأنف من آخر نقطة
        single_mode:  True عند كشط مفرد من الواجهة (يتجاوز فحص is_done)
    """
    domain = _domain(store_url)
    cp     = state.get(domain, store_url)

    # إذا اكتمل ولا نريد الإجبار → تخطِّ
    if resume and cp.status == "done" and not single_mode:
        logger.info(f"⏭️ {domain} — مكتمل ({cp.rows_saved} منتج)")
        return []

    cp.status     = "running"
    cp.started_at = cp.started_at or datetime.now().isoformat()
    state.save()

    # استيراد محلل Sitemap
    try:
        from engines.sitemap_resolve import resolve_store_product_urls
    except ImportError:
        try:
            from scrapers.sitemap_resolve import resolve_store_product_urls
        except ImportError:
            logger.error("تعذّر تحميل sitemap_resolve")
            state.mark_error(domain, "import_error")
            return []

    connector = aiohttp.TCPConnector(ssl=False, limit=concurrency + 5)
    async with aiohttp.ClientSession(
        connector=connector, timeout=aiohttp.ClientTimeout(total=30)
    ) as session:

        # ── 1. جلب قائمة روابط المنتجات ─────────────────────────────────
        progress.current_store    = domain
        progress.store_urls_done  = 0
        progress.store_urls_total = 0
        progress.save()

        logger.info(f"🗺️ {domain} — يحلل Sitemap…")
        try:
            entries = await asyncio.wait_for(
                resolve_store_product_urls(session, store_url), timeout=120
            )
        except asyncio.TimeoutError:
            state.mark_error(domain, "sitemap_timeout")
            return []
        except Exception as e:
            state.mark_error(domain, str(e)[:150])
            return []

        if not entries:
            logger.warning(f"⚠️ {domain} — لا روابط في Sitemap")
            state.mark_error(domain, "empty_sitemap")
            return []

        all_urls = [e.url for e in entries]
        total    = len(all_urls)
        if max_products and max_products < total:
            all_urls = all_urls[:max_products]
            total    = len(all_urls)

        # ── استئناف: تخطي الروابط المُعالَجة ────────────────────────────
        resume_idx = cp.last_url_index if (resume and cp.last_url_index > 0) else 0
        if resume_idx > 0:
            logger.info(f"🔄 {domain} — استئناف من الرابط {resume_idx}/{total}")
        pending_urls = all_urls[resume_idx:]

        state.update(domain, urls_total=total, urls_done=resume_idx)
        progress.urls_total       += total
        progress.store_urls_total  = total

        # ── 2. كشط بالتزامن مع حفظ دوري لنقاط الاستئناف ─────────────────
        semaphore         = asyncio.Semaphore(concurrency)
        rows: List[dict]  = []
        done_count        = resume_idx
        checkpoint_every  = max(50, min(200, total // 10 + 1))

        async def _fetch_one(url: str) -> None:
            nonlocal done_count
            try:
                row = await fetch_product(session, url, store_url, semaphore)
                if row:
                    rows.append(row)
            except Exception as e:
                progress.fetch_exceptions += 1
                progress.last_error = str(e)[:100]
            finally:
                done_count += 1
                progress.urls_processed  += 1
                progress.store_urls_done  = done_count

                if done_count % 10 == 0 or done_count >= total:
                    safe = progress.urls_processed
                    progress.success_rate_pct = (
                        (safe - progress.fetch_exceptions) / safe * 100 if safe else 0
                    )
                    progress.save()

                # ── حفظ نقطة استئناف ──
                if done_count % checkpoint_every == 0:
                    state.update(
                        domain,
                        last_url_index=done_count,
                        urls_done=done_count,
                    )
                    logger.info(
                        f"💾 {domain} — نقطة @ {done_count}/{total} | "
                        f"{len(rows)} منتج"
                    )

        # دُفعات لتجنب تشبع الذاكرة
        BATCH = 300
        for start in range(0, len(pending_urls), BATCH):
            batch = pending_urls[start: start + BATCH]
            await asyncio.gather(*[_fetch_one(u) for u in batch])

    state.mark_done(domain, len(rows))
    logger.info(f"✅ {domain} — {len(rows)} منتج")
    return rows


# ══════════════════════════════════════════════════════════════════════════════
#  كشط متجر مفرد (تُستدعى من زر الواجهة)
# ══════════════════════════════════════════════════════════════════════════════

def run_single_store(
    store_url: str,
    concurrency: int = 8,
    max_products: int = 0,
    force: bool = False,
) -> dict:
    """
    كشط متجر واحد بشكل فوري (Blocking في خيط منفصل).

    Args:
        store_url:    رابط المتجر
        concurrency:  طلبات متزامنة
        max_products: 0 = كل المنتجات
        force:        True → أعد الكشط حتى لو اكتمل

    Returns:
        {"success": bool, "rows": int, "message": str, "domain": str}
    """
    domain = _domain(store_url)
    state  = ScraperState()
    if force:
        state.reset(domain)

    progress = Progress(
        running=True,
        started_at=datetime.now().isoformat(),
        stores_total=1,
        current_store=domain,
    )
    progress.save()

    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        rows = loop.run_until_complete(
            scrape_one_store(
                store_url, progress, state,
                concurrency=concurrency,
                max_products=max_products,
                resume=not force,
                single_mode=True,
            )
        )
    except Exception as e:
        progress.running = False
        progress.save()
        state.mark_error(domain, str(e))
        return {"success": False, "rows": 0, "message": str(e), "domain": domain}
    finally:
        try:
            loop.close()
        except Exception:
            pass

    n = _merge_rows_to_csv(rows, domain)
    progress.running     = False
    progress.finished_at = datetime.now().isoformat()
    progress.stores_done = 1
    progress.rows_in_csv = n
    progress.save()

    return {
        "success": True,
        "rows":    len(rows),
        "message": f"✅ {len(rows)} منتج من {domain}",
        "domain":  domain,
    }


def _merge_rows_to_csv(new_rows: List[dict], domain: str) -> int:
    """يدمج صفوف جديدة مع OUTPUT_CSV (يستبدل بيانات المتجر القديمة)"""
    if not new_rows:
        return _count_csv_rows()

    new_df = pd.DataFrame(new_rows)
    for col in CSV_COLS:
        if col not in new_df.columns:
            new_df[col] = ""

    try:
        old_df = pd.read_csv(OUTPUT_CSV, encoding="utf-8-sig", low_memory=False)
        old_df = old_df[old_df["store"].astype(str) != domain]
        combined = pd.concat([old_df, new_df[CSV_COLS]], ignore_index=True)
    except Exception:
        combined = new_df[CSV_COLS]

    combined.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    return len(combined)


def _count_csv_rows() -> int:
    try:
        return sum(1 for _ in open(OUTPUT_CSV, encoding="utf-8-sig")) - 1
    except Exception:
        return 0


# ══════════════════════════════════════════════════════════════════════════════
#  حلقة الكشط الرئيسية (كل المتاجر)
# ══════════════════════════════════════════════════════════════════════════════

async def run_scraper(
    concurrency: int = 8,
    max_products: int = 0,
    resume: bool = True,
) -> None:
    """
    يكشط جميع المتاجر في competitors_list.json بالترتيب.
    يكتب إلى PROGRESS_FILE و STATE_FILE و OUTPUT_CSV.
    """
    try:
        with open(COMPETITORS_FILE, encoding="utf-8") as f:
            stores: List[str] = json.load(f)
    except Exception:
        stores = []

    if not stores:
        logger.error("لا توجد متاجر في competitors_list.json")
        return

    state    = ScraperState()
    progress = Progress(
        running=True,
        started_at=datetime.now().isoformat(),
        stores_total=len(stores),
    )
    progress.save()

    for i, store_url in enumerate(stores, 1):
        domain = _domain(store_url)
        logger.info(f"\n{'═'*60}\n🏪 [{i}/{len(stores)}] {domain}\n{'═'*60}")
        progress.stores_done   = i - 1
        progress.current_store = domain
        progress.save()

        rows = await scrape_one_store(
            store_url, progress, state,
            concurrency=concurrency,
            max_products=max_products,
            resume=resume,
        )

        progress.stores_done = i
        progress.stores_results[domain] = len(rows)
        progress.rows_in_csv = _merge_rows_to_csv(rows, domain)
        progress.save()

    progress.running     = False
    progress.finished_at = datetime.now().isoformat()
    progress.save()

    summary = state.get_summary()
    logger.info(
        f"\n✅ اكتمل | متاجر: {summary['done']}/{summary['total']} "
        f"| أخطاء: {summary['errors']} "
        f"| منتجات: {progress.rows_in_csv:,}"
    )


# ══════════════════════════════════════════════════════════════════════════════
#  CLI
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="محرك كشط مهووس v2.0")
    parser.add_argument("--store", default="",
                        help="رابط متجر واحد (فارغ = كل المتاجر)")
    parser.add_argument("--max-products", type=int, default=0,
                        help="أقصى عدد منتجات لكل متجر (0 = بلا حد)")
    parser.add_argument("--concurrency", type=int, default=8,
                        help="عدد الطلبات المتزامنة")
    parser.add_argument("--no-resume", action="store_true",
                        help="إعادة الكشط من الصفر (تجاهل نقاط الاستئناف)")
    parser.add_argument("--reset-state", action="store_true",
                        help="مسح كل نقاط الاستئناف قبل البدء")
    args = parser.parse_args()

    resume = not args.no_resume

    if args.reset_state:
        ScraperState().reset()
        logger.info("🗑️ تم مسح نقاط الاستئناف")

    if args.store:
        result = run_single_store(
            args.store,
            concurrency=args.concurrency,
            max_products=args.max_products,
            force=not resume,
        )
        logger.info(result["message"])
    else:
        asyncio.run(
            run_scraper(
                concurrency=args.concurrency,
                max_products=args.max_products,
                resume=resume,
            )
        )


if __name__ == "__main__":
    main()
