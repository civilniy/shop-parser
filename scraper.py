import base64
import csv
import json
import os
import re
import time
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

import gspread
from google.oauth2.service_account import Credentials


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0 Safari/537.36"
}
TIMEOUT = 30

MAX_PAGES_PER_CATALOG = 200
SLEEP_BETWEEN_REQUESTS_SEC = 0.6
SLEEP_BETWEEN_PAGES_SEC = 0.8

HEADER = ["url", "name", "category", "color", "size", "stock"]

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
    crumbs = soup.select(
        "nav.breadcrumbs a, .breadcrumbs a, [aria-label='breadcrumb'] a, .breadcrumb a"
    )
    txts = [clean_text(c.get_text(" ", strip=True)) for c in crumbs]
    txts = [t for t in txts if t]
    if len(txts) >= 2:
        return " / ".join(txts[-4:])
    return ""


def extract_product_links_from_catalog(soup: BeautifulSoup, base_url: str) -> list[str]:
    """
    Kixbox/InSales: ссылки на товары содержат /product/
    """
    urls = []
    for a in soup.select('a[href*="/product/"]'):
        href = a.get("href", "")
        if not href:
            continue
        full = urljoin(base_url, href)
        if "/product/" not in full:
            continue
        urls.append(full)

    # уникализируем с сохранением порядка
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

    # STOCK (эвристика по тексту)
    page_text = clean_text(soup.get_text(" ", strip=True))
    page_text_lower = page_text.lower()
    stock = parse_stock_from_text(page_text_lower)

    # COLOR (эвристики)
    color = ""

    # 1) строка вида "Цвет: XXX"
    for s in soup.stripped_strings:
        t = clean_text(s)
        if t.lower().startswith("цвет:"):
            cand = clean_text(t.split(":", 1)[1])
            if looks_like_color(cand):
                color = cand
                break

    # 2) блок характеристик
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

    # 3) фоллбек: первая “похожая” строка
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

    # select options
    for opt in soup.select("select option"):
        t = clean_text(opt.get_text(" ", strip=True))
        if SIZE_RE.match(t):
            sizes.append(t.upper().replace("  ", " "))

    # кнопки/ссылки размеров
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


# ---------------- Google Sheets ----------------

def init_sheet():
    """
    ВАЖНО: используем ТОЛЬКО base64 ключ.
    Это гарантирует, что json.loads не сломается из-за форматирования переменной окружения.
    """
    sheet_id = os.getenv("GOOGLE_SHEET_ID", "").strip()
    sa_b64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_B64", "").strip()

    if not sheet_id:
        raise RuntimeError("ENV GOOGLE_SHEET_ID не задан")
    if not sa_b64:
        raise RuntimeError("ENV GOOGLE_SERVICE_ACCOUNT_JSON_B64 не задан (проверь Render → Environment)")

    # Диагностика (безопасно: не печатаем ключ, только длины и префикс)
    log(f"init_sheet: GOOGLE_SHEET_ID length={len(sheet_id)}")
    log(f"init_sheet: GOOGLE_SERVICE_ACCOUNT_JSON_B64 length={len(sa_b64)}")
    log(f"init_sheet: B64 prefix={sa_b64[:12]!r}")

    try:
        sa_json = base64.b64decode(sa_b64).decode("utf-8").strip()
    except Exception as e:
        raise RuntimeError(f"init_sheet: не смог декодировать base64 ключ: {e}")

    log(f"init_sheet: decoded startswith={sa_json[:12]!r}")
    if not sa_json.startswith("{"):
        raise RuntimeError("init_sheet: декодированный ключ не похож на JSON (не начинается с '{'). "
                           "Значит base64 строка неверная или не от полного файла.")

    info = json.loads(sa_json)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)

    sh = gc.open_by_key(sheet_id)
    ws = sh.sheet1
    return ws


def sheet_reset(ws):
    ws.clear()
    ws.update([HEADER], value_input_option="RAW")


def sheet_append_rows(ws, rows: list[list[str]]):
    ws.append_rows(rows, value_input_option="RAW")


def save_csv_fallback(all_rows: list[dict], path: str = "output.csv"):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=HEADER)
        w.writeheader()
        w.writerows(all_rows)


def main():
    # батч
    try:
        batch_size = int(os.getenv("SHEETS_BATCH_SIZE", "300").strip())
    except Exception:
        batch_size = 300

    catalog_urls = load_catalog_urls()
    if not catalog_urls:
        log("catalog.txt пустой — нечего парсить.")
        return

    # Инициализируем Sheets
    ws = init_sheet()
    sheet_reset(ws)
    log("Google Sheet очищен и заголовок записан. Начинаю парсинг...")

    all_rows_dict: list[dict] = []      # запасной CSV
    buffer_rows: list[list[str]] = []   # батч на запись в Sheets
    total_written = 0

    def flush_buffer():
        nonlocal total_written, buffer_rows
        if not buffer_rows:
            return
        sheet_append_rows(ws, buffer_rows)
        total_written += len(buffer_rows)
        log(f"Записал в Google Sheets: +{len(buffer_rows)} строк (итого {total_written})")
        buffer_rows = []

    for catalog_url in catalog_urls:
        log(f"\n=== Каталог: {catalog_url} ===")

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

            for i, product_url in enumerate(product_links, 1):
                try:
                    log(f"    [{i}/{len(product_links)}] {product_url}")
                    prod = parse_product_page(product_url)

                    if prod["sizes"]:
                        for size in prod["sizes"]:
                            row = {
                                "url": prod["url"],
                                "name": prod["name"],
                                "category": prod["category"],
                                "color": prod["color"],
                                "size": size,
                                "stock": prod["stock"]
                            }
                            all_rows_dict.append(row)
                            buffer_rows.append([row[k] for k in HEADER])
                    else:
                        row = {
                            "url": prod["url"],
                            "name": prod["name"],
                            "category": prod["category"],
                            "color": prod["color"],
                            "size": "",
                            "stock": prod["stock"]
                        }
                        all_rows_dict.append(row)
                        buffer_rows.append([row[k] for k in HEADER])

                    if len(buffer_rows) >= batch_size:
                        flush_buffer()

                except Exception as e:
                    log(f"      Ошибка парсинга товара: {e}")

                time.sleep(SLEEP_BETWEEN_REQUESTS_SEC)

            time.sleep(SLEEP_BETWEEN_PAGES_SEC)

    flush_buffer()
    save_csv_fallback(all_rows_dict, "output.csv")
    log("Готово. Таблица заполнена по ходу работы, CSV сохранён как запасной вариант (output.csv).")


if __name__ == "__main__":
    main()
