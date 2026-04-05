"""
scrapers/sitemap_resolve.py — حل روابط Sitemap للمتاجر الإلكترونية
═══════════════════════════════════════════════════════════════════
يحدّد مسار Sitemap لأي متجر بأولوية:
  1. /sitemap_index.xml  (Shopify/WooCommerce)
  2. /sitemap.xml        (المعيار العام)
  3. robots.txt → سطر Sitemap:
  4. مسارات مخصصة لمتاجر سلة / زيد / Salla

يُعيد قائمة URLs لمنتجات المتجر جاهزة للكشط.
"""
import asyncio
import logging
import re
import xml.etree.ElementTree as ET
from urllib.parse import urlparse

import aiohttp

logger = logging.getLogger(__name__)

# ── ثوابت ─────────────────────────────────────────────────────────────────
_TIMEOUT = aiohttp.ClientTimeout(total=20)

_SITEMAP_CANDIDATES = [
    "/sitemap_index.xml",
    "/sitemap.xml",
    "/sitemap-products.xml",
    "/products-sitemap.xml",
    "/page-sitemap.xml",
]

# متاجر سلة: Sitemap في مسار مختلف
_SALLA_PATTERN = re.compile(
    r"(?:salla\.sa|salla\.store|salla\.store|\.myshopify\.com|\.sa/store)", re.I
)

# مؤشرات روابط منتجات (Salla/Zid/Shopify وغيرها)
_PRODUCT_URL_HINTS = re.compile(
    r"/products?/|/item/|/shop/|/product/|[/-]p\d+(?:$|[/?#])|/p\d+(?:$|[/?#])",
    re.I,
)

# استبعاد الصفحات غير المنتجات (سياسات/مدونة/حساب/دعم...)
_NON_PRODUCT_URL_HINTS = re.compile(
    r"/blog(?:/|$)|/category(?:/|$)|/collections?(?:/|$)|/pages?(?:/|$)|"
    r"privacy|policy|terms|shipping|return|refund|about|contact|faq|"
    r"wishlist|checkout|cart|login|register|account|sitemap|track|help|"
    r"سياس|الخصوص|الشحن|الارجاع|الاستبدال|اتفاقي|اتصل|المدون|الاسئله",
    re.I,
)


def _looks_like_product_url(url: str) -> bool:
    """
    يقرر هل الرابط يبدو صفحة منتج.
    """
    if not url:
        return False
    low = url.lower()
    if low.endswith(".xml"):
        return False
    if _NON_PRODUCT_URL_HINTS.search(low):
        return False
    return bool(_PRODUCT_URL_HINTS.search(low))


def _base_url(url: str) -> str:
    """يُرجع https://example.com بدون مسار."""
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}"


async def _fetch_text(session: aiohttp.ClientSession, url: str) -> str | None:
    """GET مع تجاهل أخطاء TLS وإرجاع None عند الفشل."""
    try:
        async with session.get(url, allow_redirects=True, ssl=False) as resp:
            if resp.status == 200:
                return await resp.text(errors="ignore")
    except Exception as exc:
        logger.debug("fetch_text %s → %s", url, exc)
    return None


def _parse_sitemap_urls(xml_text: str, base: str) -> list[str]:
    """
    يُحلِّل XML ويُرجع:
    - روابط <sitemap> إن كانت sitemap_index
    - روابط <url><loc> إن كانت sitemap عادي
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []

    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    urls: list[str] = []

    # sitemap index
    for loc in root.findall(".//sm:sitemap/sm:loc", ns):
        if loc.text:
            urls.append(loc.text.strip())

    # عادي
    for loc in root.findall(".//sm:url/sm:loc", ns):
        if loc.text:
            urls.append(loc.text.strip())

    # بدون namespace
    if not urls:
        for loc in root.iter("loc"):
            if loc.text:
                urls.append(loc.text.strip())

    return urls


async def _sitemap_from_robots(
    session: aiohttp.ClientSession, base: str
) -> list[str]:
    """يستخرج روابط Sitemap من robots.txt."""
    text = await _fetch_text(session, f"{base}/robots.txt")
    if not text:
        return []
    found = []
    for line in text.splitlines():
        if line.lower().startswith("sitemap:"):
            url = line.split(":", 1)[1].strip()
            if url.startswith("http"):
                found.append(url)
    return found


async def resolve_product_urls(
    store_url: str,
    session: aiohttp.ClientSession,
    *,
    max_products: int = 0,
) -> list[str]:
    """
    الدالة الرئيسية — تُرجع قائمة URLs لصفحات المنتجات.

    max_products=0 يعني بلا حد (جميع المنتجات).

    الخوارزمية:
    1. جرّب مسارات Sitemap المعيارية
    2. إن لم تجد → robots.txt
    3. فلترة URLs التي تبدو صفحات منتجات
    """
    _no_limit = (max_products <= 0)
    base = _base_url(store_url)
    product_urls: list[str] = []

    # ── جمع مرشحي Sitemap ──────────────────────────────────────────────────
    sitemap_urls: list[str] = []

    for path in _SITEMAP_CANDIDATES:
        text = await _fetch_text(session, f"{base}{path}")
        if text and ("<urlset" in text or "<sitemapindex" in text):
            parsed = _parse_sitemap_urls(text, base)
            sitemap_urls.extend(parsed)
            if parsed:
                break

    if not sitemap_urls:
        sitemap_urls = await _sitemap_from_robots(session, base)

    # ── تتبع sitemap_index متداخل (مستوى واحد فقط) ───────────────────────
    nested: list[str] = []
    for url in sitemap_urls:
        if url.endswith(".xml"):
            text = await _fetch_text(session, url)
            if text:
                nested.extend(_parse_sitemap_urls(text, base))

    all_urls = sitemap_urls + nested

    # ── فلترة صفحات المنتجات + إزالة التكرار ─────────────────────────────
    seen: set[str] = set()
    for url in all_urls:
        if url in seen:
            continue
        seen.add(url)
        if _looks_like_product_url(url):
            product_urls.append(url)
        if not _no_limit and len(product_urls) >= max_products:
            break

    # إذا فشل كل شيء، أرجع كل الـ URLs بدون فلتر
    if not product_urls and all_urls:
        product_urls = all_urls if _no_limit else all_urls[:max_products]

    logger.info("resolve_product_urls %s → %d URLs", base, len(product_urls))
    return product_urls
