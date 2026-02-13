import csv
import json
import os
import re
import time
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# --- Google Sheets ---
import gspread
from google.oauth2.service_account import Credentials

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0 Safari/537.36"
}
TIMEOUT = 30

MAX_PAGES_PER_CATALOG = 300
SLEEP_BETWEEN_REQUESTS_SEC = 0.6

SIZE_RE = re.compile(
    r"^(XS|S|M|L|XL|XXL|XXXL|XXXS|ONE SIZE|OS|O/S|"
    r"US\s?\d+([.,]\d+)?|EU\s?\d+([.,]\d+)?|"
    r"\d+([.,]\d+)?)$",
    re.IGNORECASE
)


def log(msg: str):
    print(msg, flush=True)


def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def fetch(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.text


def load_catalog_urls() -> list[str]:
    with open("catalog.txt", "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip() and not line.strip().startswith("#")]
    return urls


def catalog_page_url(catalog_url: str, page: int) -> str:
    if page == 1:
        return catalog_url
    sep = "&" if "?" in catalog_url else "?"
    return f"{catalog_url}{sep}page={page}"


def get_breadcrumb_category(soup: BeautifulSoup) -> str:
    crumbs = soup.select("nav.breadcrumbs a, .breadcrumbs a, [aria-label='breadcrumb'] a, .breadcrumb a")
    txts = [clean_text(c.get_text(" ", strip=True)) for c in crumbs]
    txts = [t for t in txts if t]
    if len(txts) >= 2:
        return " / ".join(txts[-4:])
    return ""


def extract_product_links_from_catalog(soup: BeautifulSoup, base_url: str) -> list[str]:
    urls = []
    for a in soup.select('a[href*="/product/"]'):
        href = a.get("href", "")
        if not href:
            continue
        full = urljoin(base_url, href)
        if "/product/" not in full:
            continue
        urls.append(full)

    # уникализируем, сохраняя порядок
    seen = set()
    uniq = []
    for u in urls:
        if u in seen:
            continue
        seen.add(u)
        uniq.append(u)
    return uniq


def parse_stock_from_text(page_text_lower: str) -> str:
    if "нет в наличии" in page_text_lower or "sold out" in page_text_lower:
        return "нет в наличии"
    if "в наличии" in page_text_lower or "in stock" in page_text_lower:
        return "в наличии"
    return ""


def looks_like_color(text: str) -> bool:
    t = clean_text(text)
    if not t:
        return False
    tl = t.lower()
    bad = [
        "₽", "руб", "в сплит", "добавить", "корзин", "купить",
        "нет в наличии", "в наличии", "предзаказ", "sale", "скидк",
        "размер", "цвет"
    ]
    if any(b in tl for b in bad):
        return False
    if 2 <= len(t) <= 40 and re.search(r"[A-Za-zА-Яа-я]", t):
        return True
    return False


def parse_product_page(product_url: str) -> dict:
    html = fetch(product_url)
    soup = BeautifulSoup(html, "lxml")

    # NAME
    name = ""
    h1 = soup.select_one("h1")
    if h1:
        name = clean_text(h1.get_text(" ", strip=True))

    # CATEGORY
    category = get_breadcrumb_category(soup)

    # PAGE TEXT for stock fallback
    page_text = clean_text(soup.get_text(" ", strip=True))
    page_text_lower = page_text.lower()

    # STOCK
    stock = parse_stock_from_text(page_text_lower)

    # COLOR heuristics
    color = ""

    # 1) По “Цвет:” рядом
    for s in soup.stripped_strings:
        t = clean_text(s)
        tl = t.lower()
        if tl.startswith("цвет:"):
            cand = clean_text(t.split(":", 1)[1])
            if looks_like_color(cand):
                color = cand
                break

    # 2) По спискам характеристик
    if not color:
        props = soup.select(".properties, .product-properties, .characteristics, .product-params, .product__properties")
        for p in props:
            txts = [clean_text(x) for x in p.stripped_strings]
            for i, tt in enumerate(txts):
                if tt.lower() in ("цвет", "color"):
                    if i + 1 < len(txts) and looks_like_color(txts[i + 1]):
                        color = txts[i + 1]
                        break
            if color:
                break

    # 3) Фоллбек: первая “похожая” строка
    if not color:
        candidates = []
        for s in soup.stripped_strings:
            st = clean_text(s)
            if st and looks_like_color(st) and st != name:
                candidates.append(st)
        if candidates:
            color = candidates[0]

    # SIZES
    sizes = []

    for opt in soup.select("select option"):
        t = clean_text(opt.get_text(" ", strip=True))
        if SIZE_RE.match(t):
            sizes.append(t.upper().replace("  ", " "))

    for el in soup.select("a, button, span, div"):
        t = clean_text(el.get_text(" ", strip=True))
        if not t:
            continue
        if SIZE_RE.match(t) and len(t) <= 10:
            sizes.append(t.upper().replace("  ", " "))

    sizes = list(dict.fromkeys(sizes))

    return {
        "url": product_url,
        "name": name,
        "category": category,
        "color": color,
        "sizes": sizes,
        "stock": stock
    }


def save_csv(rows: list[dict], path: str = "output.csv"):
    with open(path, "w", newline="", encoding="utf-8") as f:
        fieldnames = ["url", "name", "category", "color", "size", "stock"]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def write_to_google_sheets(rows: list[dict]):
    sheet_id = os.getenv("GOOGLE_SHEET_ID", "").strip()
    sa_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()

    if not sheet_id:
        raise RuntimeError("Нет переменной окружения GOOGLE_SHEET_ID")
    if not sa_json:
        raise RuntimeError("Нет переменной окружения GOOGLE_SERVICE_ACCOUNT_JSON")

    info = json.loads(sa_json)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)

    sh = gc.open_by_key(sheet_id)
    ws = sh.sheet1  # первая вкладка

    header = ["url", "name", "category", "color", "size", "stock"]

    values = [header]
    for r in rows:
        values.append([
            r.get("url", ""),
            r.get("name", ""),
            r.get("category", ""),
            r.get("color", ""),
            r.get("size", ""),
            r.get("stock", ""),
        ])

    # очистим и запишем
    ws.clear()
    ws.update(values, value_input_option="RAW")


def main():
    catalog_urls = load_catalog_urls()
    if not catalog_urls:
        log("catalog.txt пустой — нечего парсить.")
        return

    all_rows: list[dict] = []

    for catalog_url in catalog_urls:
        log(f"\n=== Каталог: {catalog_url} ===")

        total_products_seen = 0
        total_rows_written = 0

        for page in range(1, MAX_PAGES_PER_CATALOG + 1):
            page_url = catalog_page_url(catalog_url, page)
            log(f"Страница {page}: {page_url}")

            try:
                html = fetch(page_url)
            except Exception as e:
                log(f"  Ошибка загрузки страницы каталога: {e}")
                break

            soup = BeautifulSoup(html, "lxml")
            product_links = extract_product_links_from_catalog(soup, base_url=catalog_url)

            if not product_links:
                log("  Товаров на странице не найдено — считаю, что каталог закончился.")
                break

            log(f"  Нашёл ссылок на товары: {len(product_links)}")
            total_products_seen += len(product_links)

            for i, product_url in enumerate(product_links, 1):
                try:
                    log(f"    [{i}/{len(product_links)}] Товар: {product_url}")
                    prod = parse_product_page(product_url)

                    if prod["sizes"]:
                        for size in prod["sizes"]:
                            all_rows.append({
                                "url": prod["url"],
                                "name": prod["name"],
                                "category": prod["category"],
                                "color": prod["color"],
                                "size": size,
                                "stock": prod["stock"]
                            })
                            total_rows_written += 1
                    else:
                        all_rows.append({
                            "url": prod["url"],
                            "name": prod["name"],
                            "category": prod["category"],
                            "color": prod["color"],
                            "size": "",
                            "stock": prod["stock"]
                        })
                        total_rows_written += 1

                except Exception as e:
                    log(f"      Ошибка парсинга товара: {e}")

                time.sleep(SLEEP_BETWEEN_REQUESTS_SEC)

            time.sleep(0.8)

        log(f"Итог по каталогу: товаров увидел ~{total_products_seen}, строк добавил {total_rows_written}")

    # 1) CSV (на всякий)
    save_csv(all_rows, "output.csv")
    log("CSV готов: output.csv")

    # 2) Google Sheets (главное)
    log("Пишу в Google Sheets...")
    write_to_google_sheets(all_rows)
    log("Готово: записал в Google Sheets")


if __name__ == "__main__":
    main()
