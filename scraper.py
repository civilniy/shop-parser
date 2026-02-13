import csv
import re
import time
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0 Safari/537.36"
}
TIMEOUT = 30

MAX_PAGES_PER_CATALOG = 200
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


def extract_product_links_from_catalog(soup: BeautifulSoup, base_url: str) -> list[str]:
    """
    Собираем ссылки на товары со страницы каталога.
    На InSales обычно /product/....
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

    # STOCK (простая эвристика)
    stock = parse_stock_from_text(page_text_lower)

    # Иногда наличие видно по кнопке
    btn = soup.select_one("button, .button, .btn")
    if not stock and btn:
        bt = clean_text(btn.get_text(" ", strip=True)).lower()
        if "нет в наличии" in bt:
            stock = "нет в наличии"
        elif "в корзину" in bt or "купить" in bt:
            stock = "в наличии"

    # COLOR
    color = ""
    # 1) Попробуем найти блок "Цвет" → значение рядом
    # Часто это label + value или строка в характеристиках
    for label in soup.select("div, li, span, p"):
        t = clean_text(label.get_text(" ", strip=True))
        if not t:
            continue
        tl = t.lower()
        if tl == "цвет" or tl.startswith("цвет:"):
            # попытаемся взять следующий элемент
            nxt = label.find_next()
            if nxt:
                cand = clean_text(nxt.get_text(" ", strip=True))
                if looks_like_color(cand):
                    color = cand
                    break

    # 2) Часто цвет лежит в свойствах/характеристиках списком
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

    # 3) Последний шанс: берём “похожую” строку рядом с названием
    if not color:
        # небольшая выборка строк
        candidates = []
        for s in soup.stripped_strings:
            st = clean_text(s)
            if st and looks_like_color(st) and st != name:
                candidates.append(st)
        if candidates:
            color = candidates[0]

    # SIZES
    sizes = []

    # 1) select / option (часто на сайтах)
    for opt in soup.select("select option"):
        t = clean_text(opt.get_text(" ", strip=True))
        if SIZE_RE.match(t):
            sizes.append(t.upper().replace("  ", " "))

    # 2) кнопки размеров
    for el in soup.select("a, button, span, div"):
        t = clean_text(el.get_text(" ", strip=True))
        if not t:
            continue
        if SIZE_RE.match(t):
            # исключим мусор по длине
            if len(t) <= 10:
                sizes.append(t.upper().replace("  ", " "))

    # чистим дубли
    sizes = list(dict.fromkeys(sizes))

    return {
        "url": product_url,
        "name": name,
        "category": category,
        "color": color,
        "sizes": sizes,
        "stock": stock
    }


def main():
    catalog_urls = load_catalog_urls()
    if not catalog_urls:
        log("catalog.txt пустой — нечего парсить.")
        return

    all_rows = []

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

            # пауза между страницами каталога
            time.sleep(0.8)

        log(f"Итог по каталогу: товаров увидел ~{total_products_seen}, строк в CSV добавил {total_rows_written}")

    # SAVE CSV
    with open("output.csv", "w", newline="", encoding="utf-8") as f:
        fieldnames = ["url", "name", "category", "color", "size", "stock"]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(all_rows)

    log("\nГотово: output.csv")


if __name__ == "__main__":
    main()
