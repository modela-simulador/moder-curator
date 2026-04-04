#!/usr/bin/env python3
"""
MODÈR Product Curator v2
Web interface for curating crawled products into the MODÈR import spreadsheet.
"""

import os
import json
import io
import requests
from bs4 import BeautifulSoup
from flask import Flask, render_template, request, jsonify, send_file, redirect, url_for, Response
import threading
import queue
import openpyxl
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
import time
from datetime import datetime

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "moder-curator-2026-secret")

# ─── Auth ────────────────────────────────────────────────────────────────
USERS = {
    "demo": "demo",
}

from functools import wraps
from flask import session as flask_session

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not flask_session.get("logged_in"):
            return redirect(url_for("login_page"))
        return f(*args, **kwargs)
    return decorated

# ─── Config ──────────────────────────────────────────────────────────────
# Persistent disk on Render (survives redeploys), fallback to local dir
PERSISTENT_DIR = "/opt/render/project/src/data"
if os.path.isdir(PERSISTENT_DIR):
    DATA_DIR = PERSISTENT_DIR
else:
    DATA_DIR = os.path.dirname(os.path.abspath(__file__))
CRAWL_CACHE = os.path.join(DATA_DIR, "crawl_cache.json")
SESSION_FILE = os.path.join(DATA_DIR, "session.json")

DEFAULT_BRANDS = []  # Vacío — el usuario elige desde las sugeridas o agrega manualmente

# ─── Countries ──────────────────────────────────────────────────────────
COUNTRIES = {
    "CL": {"name": "Chile", "flag": "🇨🇱", "currency": "CLP"},
    "AR": {"name": "Argentina", "flag": "🇦🇷", "currency": "ARS"},
    "MX": {"name": "México", "flag": "🇲🇽", "currency": "MXN"},
    "CO": {"name": "Colombia", "flag": "🇨🇴", "currency": "COP"},
    "ES": {"name": "España", "flag": "🇪🇸", "currency": "EUR"},
}

# Marcas sugeridas por país — cada país tiene su propio catálogo
SUGGESTED_BRANDS_BY_COUNTRY = {
    "CL": [
        {"name": "CASSIOPEA", "domain": "cassiopeaofficial.com", "url": "https://www.cassiopeaofficial.com"},
        {"name": "PARSOME", "domain": "parsome.cl", "url": "https://www.parsome.cl"},
        {"name": "ARDE,", "domain": "wearearde.cl", "url": "https://wearearde.cl"},
        {"name": "LA COT", "domain": "lacotmuet.cl", "url": "https://lacotmuet.cl", "platform": "woocommerce"},
        {"name": "D.GARCÍA", "domain": "degarcia.cl", "url": "https://www.degarcia.cl"},
        {"name": "ANTONIA FLUXÁ", "domain": "antoniafluxa.cl", "url": "https://www.antoniafluxa.cl"},
        {"name": "OCHI AND CO.", "domain": "ochiandco.cl", "url": "https://www.ochiandco.cl"},
        {"name": "FRANCA E IO", "domain": "francaeio.cl", "url": "https://www.francaeio.cl"},
        {"name": "ATHAR", "domain": "atharshoes.cl", "url": "https://www.atharshoes.cl"},
        {"name": "AMBAR", "domain": "tiendaambar.cl", "url": "https://www.tiendaambar.cl"},
        {"name": "ANONIMATO SHOP", "domain": "anonimato.cl", "url": "https://www.anonimato.cl"},
        {"name": "LOLITA LPK", "domain": "lpk.cl", "url": "https://www.lpk.cl"},
        {"name": "MARINA MIA", "domain": "marinamia.cl", "url": "https://www.marinamia.cl"},
        {"name": "DEBUT", "domain": "debut.cl", "url": "https://www.debut.cl"},
        {"name": "VIELLA", "domain": "viella.cl", "url": "https://www.viella.cl"},
        {"name": "LORAINE HOLMES", "domain": "loraineholmes.cl", "url": "https://www.loraineholmes.cl"},
        {"name": "CANDELARIA PÉREZ", "domain": "candelariaperez.cl", "url": "https://www.candelariaperez.cl"},
        {"name": "COCO LABEL", "domain": "cocolabel.cl", "url": "https://www.cocolabel.cl"},
        {"name": "MARIA GULDMAN", "domain": "mariaguldman.cl", "url": "https://www.mariaguldman.cl"},
        {"name": "CAROLINA FLORES", "domain": "carolinafloreshandmade.cl", "url": "https://www.carolinafloreshandmade.cl"},
        {"name": "ADEU.", "domain": "adeu.cl", "url": "https://www.adeu.cl"},
        {"name": "SAINTMALE", "domain": "saintmale.com", "url": "https://www.saintmale.com"},
        {"name": "BORANGORA", "domain": "borangora.com", "url": "https://www.borangora.com"},
        {"name": "CAIS.", "domain": "caiszapatos.com", "url": "https://www.caiszapatos.com"},
        {"name": "MANTO SILVESTRE", "domain": "mantosilvestre.cl", "url": "https://www.mantosilvestre.cl"},
        {"name": "BOADELA", "domain": "boadela.cl", "url": "https://www.boadela.cl"},
    ],
    "AR": [],
    "MX": [],
    "CO": [],
    "ES": [],
}

# Flat list for backward compatibility
SUGGESTED_BRANDS = SUGGESTED_BRANDS_BY_COUNTRY.get("CL", [])

# ─── Country-aware file paths ───────────────────────────────────────────
COUNTRY_FILE = os.path.join(DATA_DIR, "active_country.json")

def load_active_country():
    if os.path.exists(COUNTRY_FILE):
        try:
            with open(COUNTRY_FILE) as f:
                data = json.load(f)
                return data.get("country", "")
        except (json.JSONDecodeError, IOError):
            pass
    return ""

def save_active_country(country_code):
    tmp_path = COUNTRY_FILE + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump({"country": country_code}, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, COUNTRY_FILE)

def get_brands_file_for_country(country_code):
    """Each country gets its own active brands file"""
    if country_code:
        return os.path.join(DATA_DIR, f"active_brands_{country_code}.json")
    return os.path.join(DATA_DIR, "active_brands.json")

def get_cache_file_for_country(country_code):
    """Each country gets its own crawl cache"""
    if country_code:
        return os.path.join(DATA_DIR, f"crawl_cache_{country_code}.json")
    return CRAWL_CACHE

BRANDS_FILE = os.path.join(DATA_DIR, "active_brands.json")

def load_active_brands(country_code=None):
    """Load active brands for a specific country (or legacy global file)"""
    if country_code:
        path = get_brands_file_for_country(country_code)
    else:
        path = BRANDS_FILE
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return DEFAULT_BRANDS

def save_active_brands(brands, country_code=None):
    """Save active brands for a specific country (or legacy global file)"""
    if country_code:
        path = get_brands_file_for_country(country_code)
    else:
        path = BRANDS_FILE
    tmp_path = path + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(brands, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, path)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15"
}

# ─── Session state ───────────────────────────────────────────────────────

def load_session():
    if os.path.exists(SESSION_FILE):
        try:
            with open(SESSION_FILE) as f:
                content = f.read().strip()
                if content:
                    return json.loads(content)
        except (json.JSONDecodeError, IOError):
            pass
    return {"accepted": [], "rejected": [], "current_index": 0, "previous_urls": []}

def save_session(session):
    tmp_path = SESSION_FILE + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(session, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, SESSION_FILE)

# ─── Crawling ────────────────────────────────────────────────────────────

def fetch_with_retry(url, max_retries=3, base_delay=2.0):
    """Fetch URL with exponential backoff retries"""
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=45, allow_redirects=True)
            if resp.status_code == 200:
                return resp
            elif resp.status_code == 429 or resp.status_code == 503:
                # Rate limited or overloaded — wait longer
                wait = base_delay * (2 ** attempt) + 1
                print(f"    ⏳ {resp.status_code} — retrying in {wait:.0f}s (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait)
            else:
                print(f"    ⚠ Status {resp.status_code}")
                return None
        except requests.exceptions.Timeout:
            wait = base_delay * (2 ** attempt)
            print(f"    ⏳ Timeout — retrying in {wait:.0f}s (attempt {attempt + 1}/{max_retries})")
            time.sleep(wait)
        except Exception as e:
            print(f"    ✗ Error: {e}")
            return None
    return None


def crawl_woocommerce(brand, progress_callback=None):
    """Fetch all products from a WooCommerce store via Store API + sitemap + HTML fallback"""
    products = []
    known_urls = set()
    base_url = brand["url"].rstrip("/")

    def log(msg):
        if progress_callback:
            progress_callback(msg)

    # ── PHASE 1: WooCommerce Store API ──────────────────────────────────
    page = 1
    while True:
        url = f"{base_url}/wp-json/wc/store/v1/products?per_page=100&page={page}"
        log(f"API página {page} de {brand['name']}... ({len(products)} productos)")

        resp = fetch_with_retry(url)
        if resp is None:
            break

        try:
            data = resp.json()
        except Exception:
            break

        if not data or not isinstance(data, list):
            break

        for p in data:
            images = p.get("images", [])
            image_url = images[0].get("src", "") if images else ""
            all_images = [img.get("src", "") for img in images[:5]]

            categories = p.get("categories", [])
            category = categories[0].get("name", "") if categories else ""

            prices = p.get("prices", {})
            price_raw = prices.get("price", "0")
            minor_unit = prices.get("currency_minor_unit", 0)
            try:
                price_int = int(price_raw)
                if minor_unit > 0:
                    price = str(price_int // (10 ** minor_unit))
                else:
                    price = str(price_int)
            except (ValueError, TypeError):
                price = price_raw

            tags = [t.get("name", "") for t in p.get("tags", [])]

            desc_html = p.get("short_description", "") or p.get("description", "") or ""
            description = ""
            if desc_html:
                soup = BeautifulSoup(desc_html, "html.parser")
                description = soup.get_text(separator=" ").strip()[:500]

            permalink = p.get("permalink", "")
            is_purchasable = p.get("is_purchasable", True)

            known_urls.add(permalink.rstrip("/").lower())
            products.append({
                "brand": brand["name"],
                "name": p.get("name", ""),
                "category": category if category else categorize(tags, p.get("name", "")),
                "price": price,
                "image_url": image_url,
                "all_images": all_images,
                "product_url": permalink,
                "description": description,
                "available": is_purchasable,
                "tags": tags[:8],
                "variants": [],
                "created_at": "",
            })

        if len(data) < 100:
            break
        page += 1
        time.sleep(2.0)

    log(f"{brand['name']}: {len(products)} vía API — buscando más en sitemap...")

    # ── PHASE 2: Product sitemap (catches products API might miss) ──────
    sitemap_urls = _fetch_woo_sitemap_urls(base_url)
    missing_urls = [u for u in sitemap_urls if u.rstrip("/").lower() not in known_urls]

    if missing_urls:
        log(f"{brand['name']}: {len(missing_urls)} productos extra en sitemap, scrapeando...")
        for i, purl in enumerate(missing_urls):
            log(f"{brand['name']}: scrapeando extra {i+1}/{len(missing_urls)}")
            scraped = _scrape_single_product_page(purl, brand)
            if scraped:
                known_urls.add(purl.rstrip("/").lower())
                products.append(scraped)
            time.sleep(1.5)

    # ── PHASE 3: HTML navigation (explore shop/catalog pages for any remaining) ──
    html_urls = _discover_product_urls_from_html(brand, known_urls)
    if html_urls:
        log(f"{brand['name']}: {len(html_urls)} productos extra en HTML, scrapeando...")
        for i, purl in enumerate(html_urls):
            log(f"{brand['name']}: scrapeando HTML {i+1}/{len(html_urls)}")
            scraped = _scrape_single_product_page(purl, brand)
            if scraped:
                known_urls.add(purl.rstrip("/").lower())
                products.append(scraped)
            time.sleep(1.5)

    log(f"✓ {brand['name']}: {len(products)} productos totales")
    return products


def _fetch_woo_sitemap_urls(base_url):
    """Try WooCommerce product sitemaps to discover all published product URLs"""
    import xml.etree.ElementTree as ET

    sitemap_paths = [
        "/product-sitemap.xml",
        "/wp-sitemap-posts-product-1.xml",
    ]
    product_urls = []
    skip_suffixes = ("/tienda/", "/shop/", "/tienda", "/shop")

    for path in sitemap_paths:
        try:
            resp = requests.get(f"{base_url}{path}", headers=HEADERS, timeout=15)
            if resp.status_code != 200:
                continue
            root = ET.fromstring(resp.content)
            ns = {'s': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
            for url_el in root.findall('s:url', ns):
                loc = url_el.find('s:loc', ns)
                if loc is not None and loc.text:
                    u = loc.text.strip()
                    # Skip non-product pages (shop index, categories)
                    if not u.endswith(skip_suffixes) and u != base_url + "/":
                        product_urls.append(u)
            if product_urls:
                break  # Found a working sitemap
        except Exception as e:
            print(f"    Sitemap {path}: {e}")
            continue

    return product_urls


def _discover_product_urls_from_html(brand, already_known):
    """Navigate shop/catalog/collection pages (with pagination) to find product links not yet known"""
    base_url = brand["url"].rstrip("/")
    domain = brand.get("domain", "")

    listing_paths = [
        "/tienda", "/shop", "/productos", "/catalogo", "/collections/all",
        "/collections", "/showroom", "/product-category", "/categoria-producto",
        "/coleccion", "/coleccion-2025", "/coleccion-2026",
        "/nueva-coleccion", "/new-collection", "/all", "/"
    ]

    candidate_links = set()

    def _extract_product_links(soup):
        """Extract product links from a parsed page"""
        found = set()
        for a in soup.find_all("a", href=True):
            href = a["href"].split("?")[0].rstrip("/")
            if href.startswith("/"):
                href = base_url + href

            # Must be same domain
            if domain and domain not in href:
                continue
            if not href.startswith("http"):
                continue

            href_lower = href.lower()

            # Skip clearly non-product pages
            if any(skip in href_lower for skip in [
                "/cart", "/carrito", "/checkout", "/account", "/login", "/register",
                "/blog", "/pages/", "/policies", "/politica", "/terminos",
                "/nosotras", "/nosotros", "/about", "/contacto", "/contact",
                ".js", ".css", ".png", ".jpg", "#", "javascript:", "mailto:",
                "/wp-login", "/wp-admin", "/feed", "/reembolso", "/envio",
                "/collections/all", "/categories", "/search", "/mi-cuenta",
                "/page/", "/categoria-producto/", "/product-category/",
            ]):
                continue

            # Skip if it's the base URL itself
            if href.rstrip("/") == base_url:
                continue

            # Check if it has product-like patterns
            is_product_pattern = any(p in href_lower for p in [
                "/products/", "/producto/", "/product/", "/p/",
                "/item/", "/tienda/", "/shop/"
            ])

            # For WooCommerce: check if link has add-to-cart data attributes
            has_product_class = False
            if a.get("class"):
                classes = " ".join(a["class"]).lower()
                has_product_class = any(c in classes for c in [
                    "product", "woocommerce", "add_to_cart"
                ])

            # Accept product-pattern links, product-class links,
            # or root-level links from known WooCommerce sites
            if is_product_pattern or has_product_class:
                found.add(href)
            elif brand.get("platform") == "woocommerce":
                # WooCommerce flat slugs: accept if it's a simple /slug path
                path_part = href.replace(base_url, "").strip("/")
                if path_part and "/" not in path_part:
                    skip_slugs = [
                        "coleccion", "collection", "categoria", "category",
                        "tienda", "shop", "carrito", "cart", "checkout",
                        "nosotras", "nosotros", "about", "contacto", "contact",
                    ]
                    if not any(path_part.lower().startswith(s) for s in skip_slugs):
                        found.add(href)
        return found

    for path in listing_paths:
        try:
            # Try the listing page + up to 5 pagination pages
            for page_num in range(1, 6):
                if page_num == 1:
                    page_url = f"{base_url}{path}"
                else:
                    # WooCommerce pagination: /tienda/page/2/
                    page_url = f"{base_url}{path}/page/{page_num}/"

                resp = requests.get(page_url, headers=HEADERS, timeout=20, allow_redirects=True)
                if resp.status_code != 200:
                    break  # No more pagination pages

                soup = BeautifulSoup(resp.text, "html.parser")
                page_links = _extract_product_links(soup)
                candidate_links.update(page_links)

                # Check if there's a next page
                has_next = bool(
                    soup.find("a", class_="next") or
                    soup.find("a", attrs={"rel": "next"}) or
                    soup.find("link", attrs={"rel": "next"}) or
                    soup.select_one(".woocommerce-pagination .next, .pagination .next, a.next.page-numbers")
                )
                if not has_next:
                    break
                time.sleep(1.0)

            time.sleep(0.5)
        except Exception:
            continue

    # Also check category pages if we found category links
    category_urls = set()
    for link in list(candidate_links):
        link_lower = link.lower()
        if any(cat in link_lower for cat in ["/categoria-producto/", "/product-category/", "/product_cat/"]):
            category_urls.add(link)
            candidate_links.discard(link)  # It's a category, not a product

    for cat_url in category_urls:
        try:
            resp = requests.get(cat_url, headers=HEADERS, timeout=20, allow_redirects=True)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                candidate_links.update(_extract_product_links(soup))
            time.sleep(1.0)
        except Exception:
            continue

    # Remove already known
    return [u for u in candidate_links if u.rstrip("/").lower() not in already_known]


def _scrape_single_product_page(url, brand):
    """Scrape a single product page for details"""
    base_url = brand["url"].rstrip("/")
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20, allow_redirects=True)
        if resp.status_code != 200:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")

        # Verify it's actually a product page (multiple signals)
        has_price = bool(soup.find(class_="price") or soup.find(attrs={"itemprop": "price"})
                         or soup.select_one("[class*='price']"))
        has_cart = bool(soup.find(class_="single_add_to_cart_button")
                        or soup.find("button", {"name": "add-to-cart"})
                        or soup.find("form", {"action": "/cart/add"})  # Shopify
                        or soup.find("form", class_="product-form"))
        og_type = soup.find("meta", property="og:type")
        is_og_product = og_type and og_type.get("content", "").lower() in ("product", "og:product")
        has_product_schema = bool(soup.find(attrs={"itemtype": lambda v: v and "Product" in v})) if True else False

        if not has_price and not has_cart and not is_og_product and not has_product_schema:
            return None

        # Extract name
        name = ""
        for selector in ["h1.product_title", "h1", ".product-title", "[itemprop='name']"]:
            el = soup.select_one(selector)
            if el:
                name = el.get_text(strip=True)
                break
        if not name:
            og_title = soup.find("meta", property="og:title")
            name = og_title["content"].split(" - ")[0].strip() if og_title else url.split("/")[-1].replace("-", " ").title()

        # Extract image
        image_url = ""
        og_img = soup.find("meta", property="og:image")
        if og_img and og_img.get("content"):
            image_url = og_img["content"]
        else:
            for sel in [".woocommerce-product-gallery img", ".product-image img", "img.wp-post-image"]:
                img = soup.select_one(sel)
                if img and img.get("src"):
                    src = img["src"]
                    if src.startswith("//"):
                        src = "https:" + src
                    elif src.startswith("/"):
                        src = base_url + src
                    image_url = src
                    break

        # Extract price (works across WooCommerce, Shopify, generic)
        import re as re_mod
        price = ""
        for price_sel in [
            ".price .woocommerce-Price-amount", "[itemprop='price']",
            ".price", ".product-price", ".current-price",
            "[class*='price']", "meta[property='product:price:amount']",
        ]:
            price_el = soup.select_one(price_sel)
            if price_el:
                if price_el.name == "meta":
                    price = price_el.get("content", "")
                else:
                    price_text = price_el.get_text(strip=True)
                    nums = re_mod.findall(r'[\d.,]+', price_text)
                    if nums:
                        price = nums[0].replace(".", "").replace(",", "")
                if price:
                    break

        # Extract description
        description = ""
        og_desc = soup.find("meta", property="og:description")
        if og_desc and og_desc.get("content"):
            description = og_desc["content"][:500]

        # All images (WooCommerce, Shopify, generic)
        all_images = [image_url] if image_url else []
        for img in soup.select(".woocommerce-product-gallery img, .product-images img, .thumbnails img, .product-gallery img, .product__media img, [class*='product'] img"):
            src = img.get("data-large_image") or img.get("data-src") or img.get("src", "")
            if src.startswith("//"):
                src = "https:" + src
            elif src.startswith("/"):
                src = base_url + src
            if src and src not in all_images:
                all_images.append(src)
            if len(all_images) >= 5:
                break

        return {
            "brand": brand["name"],
            "name": name,
            "category": categorize([], name),
            "price": price,
            "image_url": image_url,
            "all_images": all_images,
            "product_url": url,
            "description": description,
            "available": True,
            "tags": [],
            "variants": [],
            "created_at": "",
        }
    except Exception as e:
        print(f"    Error scraping {url}: {e}")
        return None


def detect_platform(brand, progress_callback=None):
    """Auto-detect store platform: Shopify, WooCommerce, Jumpseller, Tiendanube, VTEX, PrestaShop, or HTML fallback"""
    if brand.get("platform"):
        return brand["platform"]

    base_url = brand["url"].rstrip("/")

    def log(msg):
        if progress_callback:
            progress_callback(msg)

    # 1. Try Shopify API
    log(f"Probando Shopify en {brand['name']}...")
    try:
        resp = requests.get(f"{base_url}/products.json?limit=1", headers=HEADERS, timeout=15, allow_redirects=True)
        if resp.status_code == 200:
            data = resp.json()
            if "products" in data:
                return "shopify"
    except Exception:
        pass

    # 2. Try WooCommerce Store API
    log(f"Probando WooCommerce en {brand['name']}...")
    try:
        resp = requests.get(f"{base_url}/wp-json/wc/store/v1/products?per_page=1", headers=HEADERS, timeout=15, allow_redirects=True)
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list) and len(data) > 0:
                return "woocommerce"
    except Exception:
        pass

    # 3. Try Jumpseller API
    log(f"Probando Jumpseller en {brand['name']}...")
    try:
        resp = requests.get(f"{base_url}/products", headers=HEADERS, timeout=15, allow_redirects=True)
        if resp.status_code == 200 and "jumpseller" in resp.text.lower():
            return "jumpseller"
    except Exception:
        pass

    # 4. Try Tiendanube
    log(f"Probando Tiendanube en {brand['name']}...")
    try:
        resp = requests.get(base_url, headers=HEADERS, timeout=15, allow_redirects=True)
        if resp.status_code == 200:
            html = resp.text.lower()
            if "tiendanube" in html or "nuvemshop" in html:
                return "tiendanube"
    except Exception:
        pass

    # 5. Detect from homepage HTML (VTEX, PrestaShop, Magento, generic)
    log(f"Analizando HTML de {brand['name']}...")
    try:
        resp = requests.get(base_url, headers=HEADERS, timeout=15, allow_redirects=True)
        if resp.status_code == 200:
            html = resp.text.lower()
            if "vtex" in html or "vteximg" in html:
                return "vtex"
            elif "prestashop" in html or "presta" in html:
                return "prestashop"
            elif "magento" in html or "mage" in html:
                return "magento"
            # If it has product links, we can try HTML scraping
            if '/product' in html or '/productos' in html or '/collections' in html:
                return "html_scrape"
    except Exception:
        pass

    # 6. Last resort — try HTML scraping anyway
    return "html_scrape"


def crawl_html_scrape(brand, progress_callback=None):
    """Scrape products from HTML pages — uses sitemap + HTML navigation + page scraping"""
    base_url = brand["url"].rstrip("/")
    known_urls = set()
    products = []

    def log(msg):
        if progress_callback:
            progress_callback(msg)

    log(f"Scraping HTML de {brand['name']}...")

    # ── PHASE 1: Try sitemaps first (most reliable for any platform) ────
    sitemap_urls = _fetch_generic_sitemap_urls(base_url, brand.get("domain", ""))
    if sitemap_urls:
        log(f"{brand['name']}: {len(sitemap_urls)} URLs en sitemap, scrapeando...")
        for i, purl in enumerate(sitemap_urls):
            if i % 5 == 0:
                log(f"{brand['name']}: producto {i+1}/{len(sitemap_urls)} (sitemap)")
            scraped = _scrape_single_product_page(purl, brand)
            if scraped:
                known_urls.add(purl.rstrip("/").lower())
                products.append(scraped)
            time.sleep(1.5)

    # ── PHASE 2: HTML navigation (explore shop/catalog pages) ───────────
    log(f"{brand['name']}: buscando más en páginas de catálogo...")

    listing_paths = [
        "/collections/all", "/products", "/productos", "/tienda",
        "/shop", "/catalogo", "/collection/all", "/collections",
        "/categoria-producto", "/product-category", "/showroom",
        "/coleccion", "/coleccion-2025", "/coleccion-2026",
        "/new-arrivals", "/novedades", "/all", "/"
    ]

    product_links = set()

    for path in listing_paths:
        try:
            url = f"{base_url}{path}"
            resp = requests.get(url, headers=HEADERS, timeout=20, allow_redirects=True)
            if resp.status_code != 200:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            for a in soup.find_all("a", href=True):
                href = a["href"]
                if href.startswith("/"):
                    href = base_url + href
                elif not href.startswith("http"):
                    continue

                href_lower = href.lower()
                if any(skip in href_lower for skip in [
                    "/cart", "/carrito", "/checkout", "/account", "/login", "/register",
                    "/blog", "/pages/", "/policies", "/politica", "/terminos",
                    "/nosotras", "/nosotros", "/about", "/contacto", "/contact",
                    ".js", ".css", ".png", ".jpg", "#", "javascript:", "mailto:",
                    "/wp-login", "/wp-admin", "/feed", "/reembolso", "/envio",
                    "/collections/all", "/categories", "/search", "/mi-cuenta",
                ]):
                    continue

                if brand["domain"] not in href:
                    continue

                href_clean = href.split("?")[0].rstrip("/")
                if href_clean.rstrip("/").lower() in known_urls:
                    continue

                if any(pattern in href_lower for pattern in [
                    "/products/", "/producto/", "/product/", "/p/",
                    "/item/", "/tienda/", "/shop/"
                ]):
                    product_links.add(href_clean)

            if product_links:
                break

            time.sleep(1.5)
        except Exception as e:
            print(f"    Error scraping {path}: {e}")
            continue

    # Paginated listing
    page = 2
    while len(product_links) > 0 and page <= 10:
        try:
            for pag_url in [
                f"{base_url}/collections/all?page={page}",
                f"{base_url}/products?page={page}",
                f"{base_url}/tienda/page/{page}/",
                f"{base_url}/shop/page/{page}/",
            ]:
                resp = requests.get(pag_url, headers=HEADERS, timeout=15, allow_redirects=True)
                if resp.status_code != 200:
                    continue
                soup = BeautifulSoup(resp.text, "html.parser")
                new_links = 0
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    if href.startswith("/"):
                        href = base_url + href
                    href_clean = href.split("?")[0].rstrip("/")
                    if brand["domain"] in href and any(p in href.lower() for p in ["/products/", "/producto/", "/product/"]):
                        if href_clean not in product_links and href_clean.rstrip("/").lower() not in known_urls:
                            product_links.add(href_clean)
                            new_links += 1
                if new_links > 0:
                    break
                else:
                    break
            page += 1
            time.sleep(2.0)
        except Exception:
            break

    # Scrape product links found via HTML
    if product_links:
        log(f"{brand['name']}: {len(product_links)} links encontrados en HTML, extrayendo datos...")
        for i, link in enumerate(sorted(product_links)):
            if i % 5 == 0:
                log(f"{brand['name']}: producto {i+1}/{len(product_links)} (HTML)")
            scraped = _scrape_single_product_page(link, brand)
            if scraped:
                known_urls.add(link.rstrip("/").lower())
                products.append(scraped)
            time.sleep(1.5)

    if not products:
        print(f"    No products found for {brand['name']}")

    log(f"✓ {brand['name']}: {len(products)} productos totales")
    return products


def _fetch_generic_sitemap_urls(base_url, domain=""):
    """Try common sitemap locations to discover product URLs for any platform"""
    import xml.etree.ElementTree as ET

    sitemap_paths = [
        "/product-sitemap.xml",
        "/wp-sitemap-posts-product-1.xml",
        "/sitemap_products_1.xml",
        "/sitemap.xml",
    ]
    product_urls = []
    ns = {'s': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

    for path in sitemap_paths:
        try:
            resp = requests.get(f"{base_url}{path}", headers=HEADERS, timeout=15)
            if resp.status_code != 200:
                continue

            root = ET.fromstring(resp.content)

            # Check if it's a sitemap index (contains <sitemap> elements)
            sub_sitemaps = root.findall('s:sitemap', ns)
            if sub_sitemaps:
                # Find product-related sub-sitemaps
                for sm in sub_sitemaps:
                    loc = sm.find('s:loc', ns)
                    if loc is not None and 'product' in loc.text.lower():
                        try:
                            sm_resp = requests.get(loc.text, headers=HEADERS, timeout=15)
                            if sm_resp.status_code != 200:
                                continue
                            sm_root = ET.fromstring(sm_resp.content)
                            for url_el in sm_root.findall('s:url', ns):
                                loc2 = url_el.find('s:loc', ns)
                                if loc2 is not None:
                                    u = loc2.text.strip()
                                    if '/product' in u.lower() or (domain and domain in u):
                                        product_urls.append(u)
                        except Exception:
                            continue
                if product_urls:
                    break
            else:
                # Direct sitemap with <url> elements
                for url_el in root.findall('s:url', ns):
                    loc = url_el.find('s:loc', ns)
                    if loc is not None:
                        u = loc.text.strip()
                        # For product-specific sitemaps, include all URLs except shop index
                        if 'product' in path:
                            skip_suffixes = ("/tienda/", "/shop/", "/tienda", "/shop")
                            if not u.endswith(skip_suffixes) and u.rstrip("/") != base_url:
                                product_urls.append(u)
                        elif '/product' in u.lower():
                            product_urls.append(u)

            if product_urls:
                break
        except Exception:
            continue

    return product_urls


def crawl_jumpseller(brand, progress_callback=None):
    """Crawl a Jumpseller store via HTML"""
    return crawl_html_scrape(brand, progress_callback)


def crawl_tiendanube(brand, progress_callback=None):
    """Crawl a Tiendanube store via HTML"""
    return crawl_html_scrape(brand, progress_callback)


def crawl_brand(brand, progress_callback=None):
    """Fetch all products — auto-detects platform and uses the right method"""
    if progress_callback:
        progress_callback(f"Detectando plataforma de {brand['name']}...")

    platform = detect_platform(brand, progress_callback)

    if progress_callback:
        progress_callback(f"{brand['name']} → {platform.upper()} detectado")

    crawlers = {
        "shopify": crawl_shopify,
        "woocommerce": crawl_woocommerce,
        "jumpseller": crawl_jumpseller,
        "tiendanube": crawl_tiendanube,
        "vtex": crawl_html_scrape,
        "prestashop": crawl_html_scrape,
        "magento": crawl_html_scrape,
        "html_scrape": crawl_html_scrape,
    }

    crawler = crawlers.get(platform, crawl_html_scrape)
    return crawler(brand, progress_callback)


def crawl_shopify(brand, progress_callback=None):
    """Fetch all products from a Shopify store via API + sitemap + HTML fallback"""
    products = []
    known_urls = set()
    base_url = brand["url"].rstrip("/")

    def log(msg):
        if progress_callback:
            progress_callback(msg)

    # ── PHASE 1: Shopify JSON API ───────────────────────────────────────
    page = 1
    while True:
        url = f"{base_url}/products.json?limit=250&page={page}"
        log(f"API página {page} de {brand['name']}... ({len(products)} productos)")

        resp = fetch_with_retry(url)
        if resp is None:
            break

        try:
            data = resp.json()
        except Exception:
            print(f"    ✗ Invalid JSON from {brand['name']} page {page}")
            break

        page_products = data.get("products", [])
        if not page_products:
            break

        for p in page_products:
            images = p.get("images", [])
            image_url = images[0]["src"] if images else ""
            all_images = [img["src"] for img in images[:5]]

            product_type = p.get("product_type", "").strip()
            tags = p.get("tags", [])
            if isinstance(tags, str):
                tags = [t.strip() for t in tags.split(",")]

            body_html = p.get("body_html", "") or ""
            description = ""
            if body_html:
                soup = BeautifulSoup(body_html, "html.parser")
                description = soup.get_text(separator=" ").strip()[:500]

            variants = p.get("variants", [])
            available_variants = [v for v in variants if v.get("available", False)]
            price_variant = available_variants[0] if available_variants else (variants[0] if variants else {})
            price = price_variant.get("price", "")
            available = len(available_variants) > 0

            handle = p.get("handle", "")
            product_url = f"{base_url}/products/{handle}" if handle else ""

            variant_titles = []
            seen_titles = set()
            for v in variants:
                t = v.get("title", "").strip()
                if t and t != "Default Title" and t not in seen_titles:
                    seen_titles.add(t)
                    variant_titles.append(t)
            variant_titles = variant_titles[:6]

            known_urls.add(product_url.rstrip("/").lower())
            products.append({
                "brand": brand["name"],
                "name": p.get("title", ""),
                "category": product_type if product_type else categorize(tags, p.get("title", "")),
                "price": price,
                "image_url": image_url,
                "all_images": all_images,
                "product_url": product_url,
                "description": description,
                "available": available,
                "tags": tags[:8],
                "variants": variant_titles[:6],
                "created_at": p.get("created_at", ""),
            })

        page += 1
        time.sleep(2.0)

    log(f"{brand['name']}: {len(products)} vía API — buscando más en sitemap/HTML...")

    # ── PHASE 2: Shopify sitemap ────────────────────────────────────────
    sitemap_urls = _fetch_shopify_sitemap_urls(base_url)
    missing_urls = [u for u in sitemap_urls if u.rstrip("/").lower() not in known_urls]

    if missing_urls:
        log(f"{brand['name']}: {len(missing_urls)} productos extra en sitemap, scrapeando...")
        for i, purl in enumerate(missing_urls):
            log(f"{brand['name']}: scrapeando extra {i+1}/{len(missing_urls)}")
            scraped = _scrape_single_product_page(purl, brand)
            if scraped:
                known_urls.add(purl.rstrip("/").lower())
                products.append(scraped)
            time.sleep(1.5)

    # ── PHASE 3: HTML navigation ────────────────────────────────────────
    html_urls = _discover_product_urls_from_html(brand, known_urls)
    if html_urls:
        log(f"{brand['name']}: {len(html_urls)} productos extra en HTML, scrapeando...")
        for i, purl in enumerate(html_urls):
            log(f"{brand['name']}: scrapeando HTML {i+1}/{len(html_urls)}")
            scraped = _scrape_single_product_page(purl, brand)
            if scraped:
                known_urls.add(purl.rstrip("/").lower())
                products.append(scraped)
            time.sleep(1.5)

    log(f"✓ {brand['name']}: {len(products)} productos totales")
    return products


def _fetch_shopify_sitemap_urls(base_url):
    """Fetch product URLs from Shopify sitemap"""
    import xml.etree.ElementTree as ET

    product_urls = []
    try:
        # Shopify main sitemap index
        resp = requests.get(f"{base_url}/sitemap.xml", headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return []

        root = ET.fromstring(resp.content)
        ns = {'s': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

        # Find product sitemap URLs in the index
        product_sitemap_urls = []
        for sitemap in root.findall('s:sitemap', ns):
            loc = sitemap.find('s:loc', ns)
            if loc is not None and 'products' in loc.text.lower():
                product_sitemap_urls.append(loc.text)

        # If no index (direct sitemap), check for product URLs directly
        if not product_sitemap_urls:
            for url_el in root.findall('s:url', ns):
                loc = url_el.find('s:loc', ns)
                if loc is not None and '/products/' in loc.text:
                    product_urls.append(loc.text.strip())
            return product_urls

        # Fetch each product sitemap
        for sm_url in product_sitemap_urls:
            try:
                resp = requests.get(sm_url, headers=HEADERS, timeout=15)
                if resp.status_code != 200:
                    continue
                sm_root = ET.fromstring(resp.content)
                for url_el in sm_root.findall('s:url', ns):
                    loc = url_el.find('s:loc', ns)
                    if loc is not None and '/products/' in loc.text:
                        product_urls.append(loc.text.strip())
            except Exception:
                continue

    except Exception as e:
        print(f"    Shopify sitemap error: {e}")

    return product_urls


def categorize(tags, title):
    text = " ".join(tags).lower() + " " + title.lower()
    cats = {
        "Vestido": ["vestido", "dress"],
        "Blazer": ["blazer", "chaqueta", "jacket"],
        "Pantalón": ["pantalón", "pantalon", "pants"],
        "Falda": ["falda", "skirt", "pollera"],
        "Blusa": ["blusa", "top", "camiseta", "camisa", "shirt"],
        "Zapatos": ["zapato", "bota", "botin", "sandalia", "shoe", "boot", "mocasín"],
        "Bolso": ["bolso", "cartera", "bag", "tote", "clutch"],
        "Accesorio": ["accesorio", "collar", "arete", "cinturón", "pañuelo"],
        "Shorts": ["short", "shorts"],
        "Jeans": ["jean", "jeans", "denim"],
        "Pijama": ["pijama", "pajama"],
    }
    for cat, kws in cats.items():
        for kw in kws:
            if kw in text:
                return cat
    return "General"


def filter_unwanted_products(products):
    """Remove kids products and non-fashion items"""
    import re as re_mod

    EXCLUDE_KEYWORDS = [
        # Kids
        "niño", "niña", "niños", "niñas", "kids", "kid", "child", "children",
        "bebé", "bebe", "baby", "infantil", "junior",
        # Non-fashion items
        "bolsa de compras", "bolsa regalo", "gift bag", "shopping bag",
        "gift card", "tarjeta de regalo", "giftcard", "tarjeta regalo",
        "embalaje", "packaging", "envoltorio", "wrapping",
        "vela", "candle", "incienso", "incense", "difusor",
        "sticker", "llavero", "keychain", "imán", "magnet",
        "taza", "mug", "plato", "plate",
        "libro", "revista", "magazine",
        "mascota", "perro", "gato",
    ]

    # Build regex pattern with word boundaries to avoid false positives
    # e.g. "pet" should NOT match "petite", "book" should NOT match "facebook"
    pattern = re_mod.compile(
        r'\b(' + '|'.join(re_mod.escape(kw) for kw in EXCLUDE_KEYWORDS) + r')\b',
        re_mod.IGNORECASE
    )

    filtered = []
    for p in products:
        # Check name, tags, and category
        text = (p.get("name", "") + " " + p.get("category", "") + " " +
                " ".join(p.get("tags", []))).lower()

        if pattern.search(text):
            continue

        filtered.append(p)

    removed = len(products) - len(filtered)
    if removed > 0:
        print(f"  Filtered {removed} unwanted products (kids/non-fashion)")
    return filtered


def deduplicate_products(products):
    """
    Remove duplicate products based on:
    1. Same product URL (canonical dedup key)
    2. Same base name with different size/color suffix (variant dedup)
    Returns deduplicated list.
    """
    seen_urls = set()
    seen_names = set()
    unique = []

    import re as re_mod

    for p in products:
        # Primary dedup: by product URL (most reliable)
        url = p.get("product_url", "").split("?")[0].rstrip("/").lower()
        if url and url in seen_urls:
            continue

        # Secondary dedup: normalize name to catch size/color variants
        raw_name = p.get("name", "")
        clean_name = re_mod.sub(r'\s*/\s*(XS|S|M|L|XL|XXL|XXXL|\d{2,3})\s*$', '', raw_name, flags=re_mod.IGNORECASE)
        clean_name = re_mod.sub(r'\s*-\s*(Negro|Blanco|Rojo|Azul|Verde|Beige|Crudo|Café|Gris|Rosa|Nude|Burdeo|Camel|Mostaza|Terracota|Ivory|Black|White|Red|Blue|Green)\s*$', '', clean_name, flags=re_mod.IGNORECASE)
        clean_name = re_mod.sub(r'\s*talla\s*\d+\s*$', '', clean_name, flags=re_mod.IGNORECASE)
        name_key = f"{p['brand']}|{clean_name.strip().lower()}"

        if name_key in seen_names:
            continue

        if url:
            seen_urls.add(url)
        seen_names.add(name_key)
        unique.append(p)

    return unique


# Global progress state for SSE
crawl_progress = {"status": "idle", "message": "", "brand_idx": 0, "brand_total": 0,
                  "products_found": 0, "current_brand": "", "done": False}
crawl_progress_queue = queue.Queue()

# In-memory buffer for the last generated spreadsheet (survives ephemeral filesystem)
last_generated_xlsx = None


def crawl_all(brands=None):
    """Crawl all brands, deduplicate, and cache results"""
    global crawl_progress
    if brands is None:
        brands = load_active_brands()

    crawl_progress = {
        "status": "running", "message": "Iniciando...",
        "brand_idx": 0, "brand_total": len(brands),
        "products_found": 0, "current_brand": "", "done": False
    }

    all_products = []
    for i, brand in enumerate(brands):
        crawl_progress["brand_idx"] = i + 1
        crawl_progress["current_brand"] = brand["name"]
        crawl_progress["message"] = f"Crawleando {brand['name']}... ({i+1}/{len(brands)})"
        crawl_progress_queue.put(dict(crawl_progress))

        print(f"Crawling {brand['name']}...")

        def progress_cb(msg):
            crawl_progress["message"] = msg
            crawl_progress_queue.put(dict(crawl_progress))

        products = crawl_brand(brand, progress_callback=progress_cb)

        if not products:
            crawl_progress["message"] = f"⚠ {brand['name']}: sin acceso (API bloqueada o sitio no disponible)"
            crawl_progress_queue.put(dict(crawl_progress))
            if "failed_brands" not in crawl_progress:
                crawl_progress["failed_brands"] = []
            crawl_progress["failed_brands"].append(brand["name"])
            time.sleep(2.0)
            continue

        products = filter_unwanted_products(products)
        before = len(products)
        products = deduplicate_products(products)
        after = len(products)
        all_products.extend(products)

        crawl_progress["products_found"] = len(all_products)
        crawl_progress["message"] = f"✓ {brand['name']}: {after} productos únicos"
        crawl_progress_queue.put(dict(crawl_progress))

        if before != after:
            print(f"  → {before} products, {before - after} duplicates removed → {after} unique")
        else:
            print(f"  → {after} products")

        # Wait between brands to be respectful
        if i < len(brands) - 1:
            time.sleep(3.0)

    # Final cross-brand dedup
    total_before = len(all_products)
    all_products = deduplicate_products(all_products)
    if total_before != len(all_products):
        print(f"Cross-brand dedup: {total_before} → {len(all_products)}")

    # Save to cache (atomic write: write to temp file first, then rename)
    cache_data = {"products": all_products, "crawled_at": datetime.now().isoformat()}
    tmp_path = CRAWL_CACHE + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(cache_data, f, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, CRAWL_CACHE)
    print(f"Cache saved: {len(all_products)} products → {CRAWL_CACHE}")

    crawl_progress["status"] = "done"
    crawl_progress["done"] = True
    crawl_progress["products_found"] = len(all_products)
    crawl_progress["message"] = f"✅ Listo: {len(all_products)} productos únicos de {len(brands)} marcas"
    crawl_progress_queue.put(dict(crawl_progress))

    return all_products


def load_crawl_cache():
    if os.path.exists(CRAWL_CACHE):
        with open(CRAWL_CACHE) as f:
            data = json.load(f)
            return data.get("products", [])
    return None


# ─── Excel generation ────────────────────────────────────────────────────

def generate_plantilla(accepted_products, output_path, previous_rows=None):
    """Generate moder_plantilla_productos.xlsx — accumulated: previous + new accepted"""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Productos"

    headers = ["Link", "Marca", "Orden", "Posición", "Top 20", "Tendencia",
               "Categoría", "Etiqueta 1", "Etiqueta 2", "Etiqueta 3"]

    header_font = Font(name="Calibri", bold=True, color="FFFFFF", size=11)
    header_fill = PatternFill(start_color="1C1C1E", end_color="1C1C1E", fill_type="solid")
    thin_border = Border(
        left=Side(style="thin", color="CCCCCC"),
        right=Side(style="thin", color="CCCCCC"),
        top=Side(style="thin", color="CCCCCC"),
        bottom=Side(style="thin", color="CCCCCC"),
    )

    for col, h in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col, value=h)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal="center")
        cell.border = thin_border

    existing_fill = PatternFill(start_color="F5F5F5", end_color="F5F5F5", fill_type="solid")
    new_fill = PatternFill(start_color="E8F5E9", end_color="E8F5E9", fill_type="solid")

    row = 2

    # ── PART 1: Write previous rows (preserve all their data as-is)
    prev_urls = set()
    if previous_rows:
        for prev_row in previous_rows:
            link = str(prev_row.get("Link", ""))
            if not link.startswith("http"):
                continue
            prev_urls.add(link.rstrip("/"))

            col_map = {
                1: link,
                2: prev_row.get("Marca", ""),
                3: prev_row.get("Orden", ""),
                4: prev_row.get("Posición", ""),
                5: prev_row.get("Top 20", "No"),
                6: prev_row.get("Tendencia", "No"),
                7: prev_row.get("Categoría", ""),
                8: prev_row.get("Etiqueta 1", ""),
                9: prev_row.get("Etiqueta 2", ""),
                10: prev_row.get("Etiqueta 3", ""),
            }
            for col, val in col_map.items():
                cell = ws.cell(row=row, column=col, value=val if val else "")
                cell.border = thin_border
                cell.fill = existing_fill

            if link:
                ws.cell(row=row, column=1).hyperlink = link
                ws.cell(row=row, column=1).font = Font(color="666666", underline="single")

            row += 1

    # ── PART 2: Write NEW accepted products (that aren't already in previous)
    brand_groups = {}
    for p in accepted_products:
        url = p.get("product_url", "").rstrip("/")
        if url in prev_urls:
            continue  # Skip — already in previous
        brand = p["brand"]
        if brand not in brand_groups:
            brand_groups[brand] = []
        brand_groups[brand].append(p)

    # Get next position number from previous rows
    max_position = 0
    if previous_rows:
        for prev_row in previous_rows:
            pos = prev_row.get("Posición", 0)
            try:
                max_position = max(max_position, int(pos))
            except (ValueError, TypeError):
                pass

    position = max_position + 1
    new_count = 0
    for brand_name, products in brand_groups.items():
        for idx, p in enumerate(products):
            tags = p.get("tags", [])
            ws.cell(row=row, column=1, value=p["product_url"]).border = thin_border
            ws.cell(row=row, column=2, value=brand_name).border = thin_border
            ws.cell(row=row, column=3, value=idx + 1).border = thin_border
            ws.cell(row=row, column=4, value=position).border = thin_border
            ws.cell(row=row, column=5, value="No").border = thin_border
            ws.cell(row=row, column=6, value="No").border = thin_border
            ws.cell(row=row, column=7, value=p.get("category", "")).border = thin_border
            ws.cell(row=row, column=8, value=tags[0] if len(tags) > 0 else "").border = thin_border
            ws.cell(row=row, column=9, value=tags[1] if len(tags) > 1 else "").border = thin_border
            ws.cell(row=row, column=10, value=tags[2] if len(tags) > 2 else "").border = thin_border

            # Green background for new products
            for c in range(1, 11):
                ws.cell(row=row, column=c).fill = new_fill

            ws.cell(row=row, column=1).hyperlink = p["product_url"]
            ws.cell(row=row, column=1).font = Font(color="0066CC", underline="single")

            row += 1
            new_count += 1
        position += 1

    # Column widths
    widths = [55, 22, 8, 10, 8, 10, 15, 15, 15, 15]
    for col, w in enumerate(widths, 1):
        ws.column_dimensions[openpyxl.utils.get_column_letter(col)].width = w

    ws.auto_filter.ref = f"A1:J{row - 1}"
    ws.freeze_panes = "A2"

    # Save to disk
    wb.save(output_path)
    # Also save to in-memory buffer for reliable download on ephemeral filesystems
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    print(f"Plantilla: {len(previous_rows or [])} anteriores + {new_count} nuevos = {row - 2} total")
    return output_path, buf


# ─── Parse previous spreadsheet ─────────────────────────────────────────

def parse_previous_spreadsheet(file_path):
    """Extract URLs and full row data from a previously uploaded spreadsheet"""
    urls = set()
    rows_data = []  # Full row data for accumulation
    try:
        wb = openpyxl.load_workbook(file_path, read_only=True)
        ws = wb.active

        # Read headers
        headers = []
        for cell in next(ws.iter_rows(min_row=1, max_row=1, values_only=True)):
            headers.append(str(cell) if cell else "")

        for row in ws.iter_rows(min_row=2, values_only=True):
            if row and row[0] and isinstance(row[0], str) and row[0].startswith("http"):
                url = row[0].strip().rstrip("/")
                urls.add(url)
                # Store full row as dict
                row_dict = {}
                for i, val in enumerate(row):
                    if i < len(headers):
                        row_dict[headers[i]] = val if val else ""
                rows_data.append(row_dict)
    except Exception as e:
        print(f"Error parsing spreadsheet: {e}")
    return urls, rows_data


# ─── Auth Routes ─────────────────────────────────────────────────────────

@app.route("/login")
def login_page():
    if flask_session.get("logged_in"):
        return redirect(url_for("index"))
    return render_template("login.html")


@app.route("/login", methods=["POST"])
def login_action():
    data = request.json
    username = data.get("username", "").strip()
    password = data.get("password", "")

    if username in USERS and USERS[username] == password:
        flask_session["logged_in"] = True
        flask_session["username"] = username
        return jsonify({"status": "ok"})
    return jsonify({"status": "error", "error": "Usuario o contraseña incorrectos"}), 401


@app.route("/logout")
def logout():
    flask_session.clear()
    return redirect(url_for("login_page"))


# ─── Routes ──────────────────────────────────────────────────────────────

@app.route("/select-country", methods=["POST"])
@login_required
def select_country():
    """Set the active country for this session"""
    data = request.json
    country_code = data.get("country", "").upper()
    if country_code not in COUNTRIES:
        return jsonify({"error": "Invalid country"}), 400
    save_active_country(country_code)
    return jsonify({"status": "ok", "country": country_code})


@app.route("/")
@login_required
def index():
    country = load_active_country()
    if not country:
        return render_template("index.html",
                               selecting_country=True,
                               countries=COUNTRIES,
                               has_cache=False, product_count=0,
                               accepted_count=0, rejected_count=0,
                               previous_count=0, active_brands=[],
                               suggested_brands=[], default_brands=[],
                               active_country="", country_info={})
    session = load_session()
    # Load country-specific cache and brands
    cache_path = get_cache_file_for_country(country)
    products = None
    if os.path.exists(cache_path):
        try:
            with open(cache_path) as f:
                data = json.load(f)
                products = data.get("products", [])
        except (json.JSONDecodeError, IOError):
            pass
    active_brands = load_active_brands(country)
    has_cache = products is not None and len(products) > 0
    suggested = SUGGESTED_BRANDS_BY_COUNTRY.get(country, [])
    return render_template("index.html",
                           selecting_country=False,
                           countries=COUNTRIES,
                           has_cache=has_cache,
                           product_count=len(products) if products else 0,
                           accepted_count=len(session.get("accepted", [])),
                           rejected_count=len(session.get("rejected", [])),
                           previous_count=len(session.get("previous_urls", [])),
                           active_brands=active_brands,
                           suggested_brands=suggested,
                           default_brands=DEFAULT_BRANDS,
                           active_country=country,
                           country_info=COUNTRIES.get(country, {}))


@app.route("/update-brands", methods=["POST"])
def update_brands():
    """Update active brand list"""
    country = load_active_country()
    data = request.json
    brands = data.get("brands", [])
    save_active_brands(brands, country)
    return jsonify({"status": "ok", "count": len(brands)})


@app.route("/add-all-suggested", methods=["POST"])
def add_all_suggested():
    """Add all suggested brands at once for current country"""
    country = load_active_country()
    active = load_active_brands(country)
    active_domains = set(b["domain"] for b in active)
    suggested = SUGGESTED_BRANDS_BY_COUNTRY.get(country, [])
    added = 0
    for brand in suggested:
        if brand["domain"] not in active_domains:
            active.append(brand)
            active_domains.add(brand["domain"])
            added += 1
    save_active_brands(active, country)
    return jsonify({"status": "ok", "added": added, "total": len(active)})


@app.route("/add-brand", methods=["POST"])
def add_brand():
    """Add a custom brand by URL"""
    country = load_active_country()
    data = request.json
    name = data.get("name", "").strip().upper()
    url = data.get("url", "").strip()

    if not name or not url:
        return jsonify({"error": "Name and URL required"}), 400

    # Ensure URL has protocol
    if not url.startswith("http"):
        url = "https://" + url

    # Extract domain
    from urllib.parse import urlparse
    domain = urlparse(url).netloc.replace("www.", "")

    active = load_active_brands(country)
    # Check not already present
    if any(b["domain"] == domain for b in active):
        return jsonify({"error": "Brand already exists"}), 400

    new_brand = {"name": name, "domain": domain, "url": url}
    active.append(new_brand)
    save_active_brands(active, country)

    return jsonify({"status": "ok", "brand": new_brand})


@app.route("/remove-brand", methods=["POST"])
def remove_brand():
    """Remove a brand from active list"""
    country = load_active_country()
    data = request.json
    domain = data.get("domain", "")
    active = load_active_brands(country)
    active = [b for b in active if b["domain"] != domain]
    save_active_brands(active, country)
    return jsonify({"status": "ok", "count": len(active)})


@app.route("/change-country", methods=["POST"])
@login_required
def change_country():
    """Switch to a different country"""
    save_active_country("")
    return jsonify({"status": "ok"})


@app.route("/crawl", methods=["POST"])
def crawl():
    """Start crawling in background thread"""
    country = load_active_country()
    active_brands = load_active_brands(country)
    if not active_brands:
        return jsonify({"error": "No brands selected"}), 400

    # Reset session at start of new crawl
    session = load_session()
    session["current_index"] = 0
    session["accepted"] = []
    session["rejected"] = []
    save_session(session)

    # Use country-specific cache file
    cache_file = get_cache_file_for_country(country)

    def run_crawl():
        products = crawl_all(active_brands)
        # Save to country-specific cache
        cache_data = {"products": products, "crawled_at": datetime.now().isoformat(), "country": country}
        tmp_path = cache_file + ".tmp"
        with open(tmp_path, "w") as f:
            json.dump(cache_data, f, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, cache_file)
        print(f"Country cache saved: {len(products)} products → {cache_file}")
        # Reset curation index for new crawl, but preserve previous_urls/rows
        session = load_session()
        session["current_index"] = 0
        session["accepted"] = []
        session["rejected"] = []
        save_session(session)
        print(f"Session reset for new curation (previous_urls preserved: {len(session.get('previous_urls', []))})")

    t = threading.Thread(target=run_crawl, daemon=True)
    t.start()
    return jsonify({"status": "started", "brands": len(active_brands)})


@app.route("/crawl-progress")
def crawl_progress_stream():
    """SSE endpoint for crawl progress"""
    def generate():
        while True:
            try:
                progress = crawl_progress_queue.get(timeout=30)
                data = json.dumps(progress, ensure_ascii=False)
                yield f"data: {data}\n\n"
                if progress.get("done"):
                    break
            except queue.Empty:
                yield f"data: {json.dumps({'status': 'waiting', 'message': 'Procesando...'})}\n\n"

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/upload-previous", methods=["POST"])
def upload_previous():
    """Upload previous spreadsheet — becomes the accumulation base"""
    file = request.files.get("file")
    if not file:
        return jsonify({"error": "No file"}), 400

    path = os.path.join(DATA_DIR, "previous_upload.xlsx")
    file.save(path)
    urls, rows_data = parse_previous_spreadsheet(path)

    session = load_session()
    session["previous_urls"] = list(urls)
    session["previous_rows"] = rows_data  # Full row data for accumulation
    save_session(session)

    return jsonify({"status": "ok", "known_urls": len(urls), "known_products": len(rows_data)})


@app.route("/curate")
def curate():
    """Main curation interface"""
    country = load_active_country()
    session = load_session()
    # Load country-specific cache
    cache_path = get_cache_file_for_country(country)
    products = None
    if os.path.exists(cache_path):
        try:
            with open(cache_path) as f:
                data = json.load(f)
                products = data.get("products", [])
        except (json.JSONDecodeError, IOError):
            pass
    if not products:
        # Fallback to legacy global cache
        products = load_crawl_cache()
    if not products:
        return redirect(url_for("index"))

    # Filter out previously known URLs
    previous_urls = set(u.rstrip("/") for u in session.get("previous_urls", []))
    processed_urls = set()
    for p in session.get("accepted", []):
        processed_urls.add(p.get("product_url", "").rstrip("/"))
    for url in session.get("rejected", []):
        processed_urls.add(url.rstrip("/"))

    # Brand filter from query param
    brand_filter = request.args.get("brand", "")

    # Get remaining products
    remaining = []
    for p in products:
        url = p["product_url"].rstrip("/")
        if url not in previous_urls and url not in processed_urls:
            if not brand_filter or p.get("brand", "") == brand_filter:
                remaining.append(p)

    # Get all unique brands from remaining (unfiltered) for the filter UI
    all_remaining = []
    for p in products:
        url = p["product_url"].rstrip("/")
        if url not in previous_urls and url not in processed_urls:
            all_remaining.append(p)

    available_brands = {}
    for p in all_remaining:
        b = p.get("brand", "Desconocida")
        available_brands[b] = available_brands.get(b, 0) + 1
    # Sort by count descending
    sorted_brands = sorted(available_brands.items(), key=lambda x: -x[1])

    if not remaining:
        if brand_filter:
            # No more in this brand, but maybe others
            return redirect(url_for("curate"))
        return render_template("done.html",
                               accepted_count=len(session.get("accepted", [])),
                               total=len(products))

    current = remaining[0]
    progress = len(products) - len(all_remaining)

    return render_template("curate.html",
                           product=current,
                           remaining=len(remaining),
                           progress=progress,
                           total=len(products),
                           accepted_count=len(session.get("accepted", [])),
                           rejected_count=len(session.get("rejected", [])),
                           available_brands=sorted_brands,
                           current_brand_filter=brand_filter)


@app.route("/action", methods=["POST"])
def action():
    """Handle accept/reject/finish"""
    data = request.json
    act = data.get("action")
    product = data.get("product")
    session = load_session()

    if act == "accept" and product:
        session["accepted"].append(product)
    elif act == "reject" and product:
        session["rejected"].append(product.get("product_url", ""))
    elif act == "skip_brand":
        # Reject ALL remaining products from this brand
        brand_to_skip = data.get("brand", "")
        if brand_to_skip:
            country = load_active_country()
            cache_file = get_cache_file_for_country(country)
            if os.path.exists(cache_file):
                with open(cache_file) as f:
                    products = json.load(f).get("products", [])
            else:
                products = load_crawl_cache() or []
            previous_urls = set(u.rstrip("/") for u in session.get("previous_urls", []))
            processed = set(p.get("product_url", "").rstrip("/") for p in session.get("accepted", []))
            processed.update(u.rstrip("/") for u in session.get("rejected", []))
            for p in products:
                url = p["product_url"].rstrip("/")
                if p.get("brand") == brand_to_skip and url not in previous_urls and url not in processed:
                    session["rejected"].append(url)
            save_session(session)
            return jsonify({"status": "ok", "skipped_brand": brand_to_skip})
    elif act == "finish":
        global last_generated_xlsx
        # Generate accumulated spreadsheet: previous + new
        output_path = os.path.join(DATA_DIR, "moder_plantilla_productos.xlsx")
        previous_rows = session.get("previous_rows", [])
        try:
            _, last_generated_xlsx = generate_plantilla(session["accepted"], output_path, previous_rows=previous_rows)
        except Exception as e:
            print(f"Error generating spreadsheet: {e}")
            import traceback
            traceback.print_exc()
            return jsonify({"status": "error", "error": str(e)}), 500
        total = len(previous_rows) + len(session["accepted"])
        accepted_count = len(session["accepted"])
        # Reset session after generating spreadsheet
        session["current_index"] = 0
        session["accepted"] = []
        session["rejected"] = []
        save_session(session)
        return jsonify({"status": "done", "count": accepted_count,
                        "previous": len(previous_rows), "total": total, "path": output_path})

    save_session(session)
    return jsonify({"status": "ok", "accepted": len(session["accepted"])})


@app.route("/download")
@login_required
def download():
    """Download generated spreadsheet — tries disk, memory, then regenerates from session"""
    global last_generated_xlsx
    path = os.path.join(DATA_DIR, "moder_plantilla_productos.xlsx")

    # Try disk file first
    if os.path.exists(path):
        try:
            return send_file(path, as_attachment=True, download_name="moder_plantilla_productos.xlsx")
        except Exception as e:
            print(f"Error sending file from disk: {e}")

    # Fallback: serve from in-memory buffer (works on ephemeral filesystems like Render)
    if last_generated_xlsx is not None:
        try:
            last_generated_xlsx.seek(0)
            return send_file(
                last_generated_xlsx,
                as_attachment=True,
                download_name="moder_plantilla_productos.xlsx",
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        except Exception as e:
            print(f"Error sending file from memory: {e}")

    # Last resort: regenerate from session data if available
    session = load_session()
    if session.get("accepted"):
        try:
            previous_rows = session.get("previous_rows", [])
            _, last_generated_xlsx = generate_plantilla(session["accepted"], path, previous_rows=previous_rows)
            last_generated_xlsx.seek(0)
            return send_file(
                last_generated_xlsx,
                as_attachment=True,
                download_name="moder_plantilla_productos.xlsx",
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        except Exception as e:
            print(f"Error regenerating spreadsheet: {e}")
            import traceback
            traceback.print_exc()
            return f"<html><body><h2>Error al generar planilla</h2><p>{e}</p><a href='/'>Volver</a></body></html>", 500

    return "<html><body><h2>No se ha generado la planilla aún</h2><p>Primero acepta productos y presiona Finalizar.</p><a href='/'>Volver al inicio</a></body></html>", 404


@app.route("/reset", methods=["POST"])
def reset():
    """Reset everything — session, cache, brands"""
    for f in [SESSION_FILE, CRAWL_CACHE, BRANDS_FILE,
              os.path.join(DATA_DIR, "previous_upload.xlsx"),
              os.path.join(DATA_DIR, "moder_plantilla_productos.xlsx")]:
        if os.path.exists(f):
            os.remove(f)
    return jsonify({"status": "ok"})


# ─── Run ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    os.makedirs(os.path.join(DATA_DIR, "templates"), exist_ok=True)
    print("\n🎨 MODÈR Product Curator v2")
    print("   http://localhost:5050\n")
    app.run(host="0.0.0.0", port=5050, debug=False)
