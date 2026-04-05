"""
scrapers/async_scraper.py — محرك الكشط غير المتزامن v2.0 (2026)
══════════════════════════════════════════════════════════════════
• يقرأ قائمة المتاجر من data/competitors_list.json
• يحدّد Sitemap لكل متجر عبر sitemap_resolve.py
• يستخرج بيانات المنتج بطبقات: JSON-LD → BeautifulSoup meta → regex fallback
• يكتب النتائج في data/competitors_latest.csv
• يحدّث data/scraper_progress.json لحظياً (للـ Dashboard)
• يدعم lastmod للكشط التزايدي (يكشط فقط الصفحات المحدّثة)
• ضد الحظر: adaptive rate limiting + curl_cffi + cloudscraper

التشغيل:
  python -m scrapers.async_scraper
  python -m scrapers.async_scraper --max-products 500 --concurrency 5
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import aiohttp
from aiohttp import ClientTimeout

from scrapers.anti_ban import (
    get_browser_headers,
    get_rate_limiter,
    fetch_with_retry,
    try_all_sync_fallbacks,
)

# ── مسارات ────────────────────────────────────────────────────────────────
_ROOT = Path(__file__).resolve().parent.parent
_DATA_DIR = (
    Path(os.environ.get("DATA_DIR", "")).resolve()
    if os.environ.get("DATA_DIR")
    else _ROOT / "data"
)
_DATA_DIR.mkdir(parents=True, exist_ok=True)

COMPETITORS_FILE = _DATA_DIR / "competitors_list.json"
OUTPUT_CSV       = _DATA_DIR / "competitors_latest.csv"
PROGRESS_FILE    = _DATA_DIR / "scraper_progress.json"
LASTMOD_FILE     = _DATA_DIR / "scraper_lastmod_cache.json"

# ── إعداد logging ──────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("scraper")

# ── Regexes للاستخراج ─────────────────────────────────────────────────────
_JSON_LD_RE   = re.compile(
    r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.S | re.I,
)
_OG_TITLE_RE  = re.compile(
    r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']', re.I
)
_OG_PRICE_RE  = re.compile(
    r'<meta[^>]+property=["\']product:price:amount["\'][^>]+content=["\']([^"\']+)["\']', re.I
)
_OG_IMAGE_RE  = re.compile(
    r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']', re.I
)
_PRICE_META_RE = re.compile(
    r'<meta[^>]+(?:itemprop|property)=["\'](?:price|product:price:amount)["\'][^>]+content=["\']([^"\']+)["\']',
    re.I,
)


# ══════════════════════════════════════════════════════════════════════════
#  Lastmod Cache — للكشط التزايدي
# ══════════════════════════════════════════════════════════════════════════
def _load_lastmod_cache() -> Dict[str, str]:
    try:
        return json.loads(LASTMOD_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_lastmod_cache(cache: Dict[str, str]) -> None:
    try:
        LASTMOD_FILE.write_text(
            json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    except Exception as exc:
        logger.warning("فشل حفظ lastmod cache: %s", exc)


# ══════════════════════════════════════════════════════════════════════════
#  Progress — يُحدِّث ملف JSON الحي
# ══════════════════════════════════════════════════════════════════════════
class Progress:
    def __init__(self, stores: List[str], total_urls: int):
        self._file = PROGRESS_FILE
        self._data: Dict[str, Any] = {
            "running":           True,
            "started_at":        datetime.now().isoformat(),
            "updated_at":        datetime.now().isoformat(),
            "stores_total":      len(stores),
            "stores_done":       0,
            "urls_total":        total_urls,
            "urls_processed":    0,
            "rows_in_csv":       0,
            "fetch_exceptions":  0,
            "success_rate_pct":  0,
            "current_store":     "",
            "last_error":        "",
            "output_file":       str(OUTPUT_CSV),
            "store_urls_total":  0,
            "store_urls_done":   0,
            "store_started_at":  "",
            "stores_results":    {},
        }
        self._flush()

    def update(self, **kwargs: Any) -> None:
        self._data.update(kwargs)
        self._data["updated_at"] = datetime.now().isoformat()
        if self._data["urls_processed"] > 0:
            ok = self._data["rows_in_csv"]
            tot = self._data["urls_processed"]
            self._data["success_rate_pct"] = round(ok / tot * 100, 1)
        self._flush()

    def done(self, rows: int) -> None:
        self._data.update({
            "running":        False,
            "rows_in_csv":    rows,
            "updated_at":     datetime.now().isoformat(),
            "finished_at":    datetime.now().isoformat(),
            "current_store":  "",
        })
        self._flush()

    def error(self, msg: str) -> None:
        self._data["last_error"] = msg[:500]
        self._data["fetch_exceptions"] = self._data.get("fetch_exceptions", 0) + 1
        self._flush()

    def _flush(self) -> None:
        try:
            PROGRESS_FILE.write_text(
                json.dumps(self._data, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as exc:
            logger.warning("progress flush failed: %s", exc)


# ══════════════════════════════════════════════════════════════════════════
#  استخراج بيانات المنتج من HTML
# ══════════════════════════════════════════════════════════════════════════
def _parse_price(raw: Any) -> Optional[float]:
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw) if float(raw) > 0 else None
    s = re.sub(r"[^\d.,]", "", str(raw).replace(",", ""))
    if not s:
        return None
    try:
        v = float(s)
        return v if v > 0 else None
    except ValueError:
        return None


def _extract_from_jsonld(html: str, page_url: str) -> Optional[Dict[str, Any]]:
    """أفضل مصدر: JSON-LD schema.org/Product — يدعم @graph وقوائم وProductGroup."""
    for block in _JSON_LD_RE.findall(html):
        try:
            data = json.loads(block)
        except json.JSONDecodeError:
            continue

        node = _find_product_node(data)
        if node is None:
            continue

        name = node.get("name")
        if isinstance(name, dict):
            name = name.get("value") or name.get("text") or name.get("@value") or str(name)
        name = str(name or "").strip()
        if not name:
            continue

        # سعر
        offers = node.get("offers") or node.get("Offers") or {}
        if isinstance(offers, list):
            offers = offers[0] if offers else {}
        if isinstance(offers, dict):
            otype = offers.get("@type", "")
            if otype == "AggregateOffer":
                price_raw = offers.get("lowPrice") or offers.get("highPrice") or offers.get("price")
            else:
                price_raw = offers.get("price")
        else:
            price_raw = offers
        if price_raw is None:
            price_raw = node.get("price")
        price = _parse_price(price_raw)
        if price is None or price <= 0:
            continue

        # صورة
        img_field = node.get("image", "")
        if isinstance(img_field, list):
            image = str(img_field[0]).strip() if img_field else ""
        elif isinstance(img_field, dict):
            image = str(img_field.get("url") or img_field.get("contentUrl") or "").strip()
        else:
            image = str(img_field or "").strip()

        # ماركة
        brand_field = node.get("brand") or {}
        if isinstance(brand_field, dict):
            brand = str(brand_field.get("name") or brand_field.get("@value") or "").strip()
        elif isinstance(brand_field, list) and brand_field:
            b0 = brand_field[0]
            brand = str(b0.get("name") if isinstance(b0, dict) else b0).strip()
        else:
            brand = str(brand_field or "").strip()

        url_out = str(node.get("url") or node.get("@id") or page_url).strip()
        sku = str(node.get("sku") or node.get("productID") or node.get("mpn") or "").strip()

        return {
            "name": name, "price": price, "image": image,
            "url": url_out or page_url, "brand": brand, "sku": sku,
        }

    return None


def _find_product_node(obj: Any) -> Optional[dict]:
    """أول كائن JSON-LD من نوع Product/ProductGroup (يشمل @graph وقوائم متداخلة)."""
    if isinstance(obj, list):
        for item in obj:
            found = _find_product_node(item)
            if found is not None:
                return found
        return None
    if isinstance(obj, dict):
        t = obj.get("@type")
        types = t if isinstance(t, list) else ([t] if t else [])
        if "Product" in types or "ProductGroup" in types:
            if "ProductGroup" in types and "Product" not in types:
                hv = obj.get("hasVariant")
                if isinstance(hv, list) and hv and isinstance(hv[0], dict):
                    return hv[0]
            return obj
        if "@graph" in obj:
            found = _find_product_node(obj["@graph"])
            if found is not None:
                return found
        for v in obj.values():
            if isinstance(v, (dict, list)):
                found = _find_product_node(v)
                if found is not None:
                    return found
    return None


def _extract_from_og(html: str, page_url: str) -> Optional[Dict[str, Any]]:
    """Fallback: Open Graph meta tags."""
    name_m = _OG_TITLE_RE.search(html)
    price_m = _OG_PRICE_RE.search(html) or _PRICE_META_RE.search(html)
    image_m = _OG_IMAGE_RE.search(html)
    if not name_m or not price_m:
        return None
    price = _parse_price(price_m.group(1))
    if price is None:
        return None
    return {
        "name":  name_m.group(1).strip(),
        "price": price,
        "image": image_m.group(1) if image_m else "",
        "url":   page_url,
        "brand": "",
        "sku":   "",
    }


def _extract_from_html_patterns(html: str, page_url: str) -> Optional[Dict[str, Any]]:
    """Fallback أخير: أنماط سعر واسم في HTML الخام."""
    price_m = re.search(r'"price"\s*:\s*"?([\d.,]+)"?', html, re.I)
    if not price_m:
        return None
    price = _parse_price(price_m.group(1))
    if price is None:
        return None
    name_m = _OG_TITLE_RE.search(html)
    if not name_m:
        title_m = re.search(r"<title[^>]*>([^<]+)</title>", html, re.I)
        if not title_m:
            return None
        name = title_m.group(1).strip()
    else:
        name = name_m.group(1).strip()
    if not name:
        return None
    image_m = _OG_IMAGE_RE.search(html)
    return {
        "name": name, "price": price,
        "image": image_m.group(1) if image_m else "",
        "url": page_url, "brand": "", "sku": "",
    }


def extract_product(html: str, page_url: str) -> Optional[Dict[str, Any]]:
    """يستخرج بيانات المنتج بثلاث طبقات: JSON-LD → OG → regex."""
    return (
        _extract_from_jsonld(html, page_url)
        or _extract_from_og(html, page_url)
        or _extract_from_html_patterns(html, page_url)
    )


# ══════════════════════════════════════════════════════════════════════════
#  جلب صفحة منتج واحدة — retry + anti-ban + fallbacks
# ══════════════════════════════════════════════════════════════════════════
async def fetch_product(
    session: aiohttp.ClientSession,
    url: str,
    store_domain: str,
    sem: asyncio.Semaphore,
    progress: Progress,
) -> Optional[Dict[str, Any]]:
    async with sem:
        resp = await fetch_with_retry(
            session, url, max_retries=3, referer=f"https://{store_domain}/"
        )

        html: Optional[str] = None
        if resp is not None:
            try:
                html = await resp.text(errors="ignore")
            except Exception:
                html = None

        if not html:
            loop = asyncio.get_event_loop()
            html = await loop.run_in_executor(None, try_all_sync_fallbacks, url)

        progress.update(urls_processed=progress._data["urls_processed"] + 1)

        if not html:
            progress.error(f"{url} → تعذّر الجلب بعد كل المحاولات")
            return None

        product = extract_product(html, url)
        if product:
            product["store"] = store_domain
            product["scraped_at"] = datetime.now().strftime("%Y-%m-%d")
        return product


# ══════════════════════════════════════════════════════════════════════════
#  المحرك الرئيسي
# ══════════════════════════════════════════════════════════════════════════
async def run_scraper(
    max_products_per_store: int = 0,
    concurrency: int = 8,
    incremental: bool = True,
) -> int:
    """
    يشغّل الكشط الكامل ويُرجع عدد الصفوف المكتوبة.

    max_products_per_store=0 → كشط جميع المنتجات بدون سقف.
    incremental=True → يتخطى الصفحات التي لم يتغير lastmod.
    """
    if not COMPETITORS_FILE.exists():
        logger.error("ملف المنافسين غير موجود: %s", COMPETITORS_FILE)
        return 0

    stores: List[str] = json.loads(COMPETITORS_FILE.read_text(encoding="utf-8"))
    if not stores:
        logger.error("قائمة المتاجر فارغة")
        return 0

    logger.info(
        "بدء الكشط — %d متجر، حد %d منتج/متجر، تزامن %d، تزايدي=%s",
        len(stores), max_products_per_store, concurrency, incremental,
    )

    # Lastmod cache للكشط التزايدي
    lastmod_cache = _load_lastmod_cache() if incremental else {}

    csv_cols = ["store", "name", "price", "image", "url", "brand", "sku", "scraped_at"]
    csv_fh = OUTPUT_CSV.open("w", newline="", encoding="utf-8-sig")
    writer = csv.DictWriter(csv_fh, fieldnames=csv_cols, extrasaction="ignore")
    writer.writeheader()

    progress = Progress(stores, total_urls=len(stores) * max(max_products_per_store, 500))
    rows_written = 0
    sem = asyncio.Semaphore(concurrency)
    new_lastmod_cache: Dict[str, str] = {}

    from scrapers.sitemap_resolve import resolve_product_entries

    connector = aiohttp.TCPConnector(ssl=False, limit=concurrency + 4)
    timeout = ClientTimeout(total=30, connect=10)
    default_headers = get_browser_headers()

    async with aiohttp.ClientSession(
        connector=connector, timeout=timeout, headers=default_headers
    ) as session:
        for store_url in stores:
            domain = urlparse(store_url).netloc
            progress.update(
                current_store=domain,
                stores_done=progress._data["stores_done"],
                store_urls_total=0,
                store_urls_done=0,
                store_started_at=datetime.now().isoformat(),
            )
            logger.info("↳ %s", domain)

            try:
                entries = await resolve_product_entries(
                    store_url, session, max_products=max_products_per_store
                )
            except Exception as exc:
                progress.error(f"resolve {domain}: {exc}")
                entries = []

            if not entries:
                logger.warning("لا روابط منتجات لـ %s", domain)
                _res = dict(progress._data.get("stores_results") or {})
                _res[domain] = 0
                progress.update(
                    stores_done=progress._data["stores_done"] + 1,
                    stores_results=_res,
                )
                continue

            # فلترة تزايدية: تخطي الصفحات التي لم يتغير lastmod
            urls_to_fetch: List[str] = []
            for entry in entries:
                if incremental and entry.lastmod:
                    cached = lastmod_cache.get(entry.url, "")
                    if cached == entry.lastmod:
                        continue
                    new_lastmod_cache[entry.url] = entry.lastmod
                urls_to_fetch.append(entry.url)

            if not urls_to_fetch and entries:
                logger.info(
                    "%s — %d منتج بلا تحديث (lastmod لم يتغير) → تخطّى",
                    domain, len(entries),
                )
                _res = dict(progress._data.get("stores_results") or {})
                _res[domain] = 0
                progress.update(
                    stores_done=progress._data["stores_done"] + 1,
                    stores_results=_res,
                )
                continue

            logger.info(
                "%s — %d للكشط (من %d في Sitemap)",
                domain, len(urls_to_fetch), len(entries),
            )

            progress.update(
                store_urls_total=len(urls_to_fetch), store_urls_done=0
            )

            tasks = [
                fetch_product(session, url, domain, sem, progress)
                for url in urls_to_fetch
            ]

            _store_rows = 0
            for _url_idx, coro in enumerate(asyncio.as_completed(tasks)):
                result = await coro
                progress.update(store_urls_done=_url_idx + 1)
                if result:
                    writer.writerow(result)
                    rows_written += 1
                    _store_rows += 1
                    if rows_written % 50 == 0:
                        csv_fh.flush()
                        progress.update(rows_in_csv=rows_written)

            _res = dict(progress._data.get("stores_results") or {})
            _res[domain] = _store_rows
            progress.update(
                stores_done=progress._data["stores_done"] + 1,
                rows_in_csv=rows_written,
                stores_results=_res,
                store_urls_done=0,
                store_urls_total=0,
            )
            logger.info("  ✓ %s — %d منتج", domain, _store_rows)

    csv_fh.close()

    # حفظ الـ lastmod cache
    if incremental:
        merged = {**lastmod_cache, **new_lastmod_cache}
        _save_lastmod_cache(merged)

    progress.done(rows_written)
    logger.info("اكتمل الكشط — %d منتج في %s", rows_written, OUTPUT_CSV)
    return rows_written


# ══════════════════════════════════════════════════════════════════════════
#  نقطة الدخول عند التشغيل المباشر
# ══════════════════════════════════════════════════════════════════════════
def main() -> None:
    parser = argparse.ArgumentParser(description="Async Competitor Scraper v2.0")
    parser.add_argument(
        "--max-products", type=int, default=0,
        help="أقصى عدد منتجات لكل متجر (0 = جميع المنتجات بلا سقف)",
    )
    parser.add_argument(
        "--concurrency", type=int, default=8,
        help="عدد الطلبات المتزامنة",
    )
    parser.add_argument(
        "--full", action="store_true",
        help="كشط كامل (يتخطى الـ lastmod cache)",
    )
    args = parser.parse_args()

    _DATA_DIR.mkdir(parents=True, exist_ok=True)

    try:
        rows = asyncio.run(
            run_scraper(
                max_products_per_store=args.max_products,
                concurrency=args.concurrency,
                incremental=not args.full,
            )
        )
        sys.exit(0 if rows > 0 else 1)
    except KeyboardInterrupt:
        try:
            prog = json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
            prog["running"] = False
            prog["last_error"] = "⛔ أُوقف يدوياً"
            PROGRESS_FILE.write_text(
                json.dumps(prog, ensure_ascii=False, indent=2), encoding="utf-8"
            )
        except Exception:
            pass
        logger.info("الكشط أُوقف يدوياً")
        sys.exit(0)
    except Exception as exc:
        logger.error("خطأ فادح: %s", exc)
        try:
            err_state = {
                "running": False, "last_error": str(exc)[:500],
                "updated_at": datetime.now().isoformat(),
            }
            PROGRESS_FILE.write_text(
                json.dumps(err_state, ensure_ascii=False, indent=2), encoding="utf-8"
            )
        except Exception:
            pass
        sys.exit(1)


if __name__ == "__main__":
    main()
