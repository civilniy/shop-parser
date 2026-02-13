import csv
import re
import time
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

HEADERS = {"User-Agent": "Mozilla/5.0"}
TIMEOUT = 30

# Ограничение, чтобы случайно не уйти в бесконечный обход
MAX_PAGES_PER_CATALOG = 200

SIZE_RE = re.compile(
    r"^(XS|S|M|L|XL|XXL|XXXL|ONE SIZE|US\d+([.,]\d+)?|EU\d+([.,]\d+)?|\d+([.,]\d+)?)$",
    re.IGNORECASE
)


def load_catalog_urls():
    with open("catalog.txt", "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip() and not line.strip().startswith("#")]


def fetch(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.text


def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def get_breadcrumb_category(soup: BeautifulSoup) -> str:
    crumbs = soup.select("nav.breadcrumbs a, .breadcrumbs a, [aria-label='breadcrumb'] a")
    txts = [clean_text(c.get_text(" ", strip=True)) for c in crumbs]
    txts = [t for t in txts if t]
    # обычно последние элементы — путь раздела
    if len(txts) >= 2:
        return " / ".join(txts[-4:])
    return ""


def looks_like_color(text: str) -> bool:
    t = clean_text(text)
    if not t:
        return False
    tl = t.lower()
    # отсекаем мусор
    bad = ["₽", "в сплит", "нет в наличии", "в наличии", "добавить", "предзаказ"]
    if any(b in tl for b in bad):
        return False
    # часто цвет/расцветка капсом или с запятыми/слешами
    if "," in t or "/" in t:
        return True
    if t.isupper() and len(t) <= 60:
        return True
    # просто короткая “расцветка”
    if 3 <= len(t) <= 35 and re.search(r"[A-Za-zА-Яа-я]", t):
        return True
    return False


def extract_product_blocks(soup: BeautifulSoup, base_url: str):
    """
    Достаём товары прямо из каталога:
    - ссылка /product/...
    - название (текст ссылки)
    - цвет/расцветка (похоже на строку рядом)
    - размеры (в карточке)
    - наличие (если в карточке есть "Нет в наличии")
    """
    category = get_breadcrumb_category(soup)
    items = []

    # все ссылки на товары
    links = soup.select('a[href*="/product/"]')
    seen_urls = set()

    for a in links:
        href = a.get("href", "")
        if not href:
            continue
        # нормализуем в абсолютную ссылку
        product_url = urljoin(base_url, href)

        if "/product/" not in product_url:
            continue
        if product_url in seen_urls:
            continue

        name = clean_text(a.get_text(" ", strip=True))
        if not name or len(name) < 3:
            continue

        # контейнер карточки: поднимаемся вверх на несколько уровней
        container = a
        for _ in range(6):
            if container.parent:
                container = container.parent
            else:
                break

        block_text = clean_text(container.get_text(" ", strip=True))
        block_text_l = block_text.lower()

        stock = ""
        if "нет в наличии" in block_text_l:
            stock = "нет в наличии"
        elif "в наличии" in block_text_l:
            stock = "в наличии"

        # размеры: собираем все "похожие на размеры" тексты ссылок внутри карточки
        sizes = []
        for s in container.select("a"):
            t = clean_text(s.get_text(" ", strip=True))
            if not t:
                continue
            if SIZE_RE.match(t):
                sizes.append(t.upper().replace("US", "US").replace("EU", "EU"))
        # убираем дубли
        sizes = list(dict.fromkeys(sizes))

        # цвет: ищем подходящую строку внутри карточки
        color = ""
        # пробуем пройтись по “кускам” текста внутри контейнера
        candidates = []
        for txt in container.stripped_strings:
            t = clean_text(txt)
            if t and looks_like_color(t) and t != name:
                candidates.append(t)
        if candidates:
            # часто первая такая строка — и есть расцветка
            color = candidates[0]

        items.append({
            "url": product_url,
            "name": name,
            "category": category,
            "color": color,
            "sizes": sizes,
            "stock": stock
        })
        seen_urls.add(product_url)

    return items


def catalog_page_url(catalog_url: str, page: int) -> str:
    if page == 1:
        return catalog_url
    sep = "&" if "?" in catalog_url else "?"
    return f"{catalog_url}{sep}page={page}"


def main():
    catalog_urls = load_catalog_urls()
    all_rows = []

    for catalog_url in catalog_urls:
        print(f"\n=== Каталог: {catalog_url} ===")
        page = 1
        total_found = 0

        while page <= MAX_PAGES_PER_CATALOG:
            url = catalog_page_url(catalog_url, page)
            print(f"Страница {page}: {url}")

            html = fetch(url)
            soup = BeautifulSoup(html, "lxml")

            items = extract_product_blocks(soup, base_url=catalog_url)
            if not items:
                print("Товаров не найдено — останавливаюсь на этом каталоге.")
                break

            total_found += len(items)

            # раскладываем “по размерам” в CSV (1 строка = 1 размер)
            for it in items:
                if it["sizes"]:
                    for size in it["sizes"]:
                        all_rows.append({
                            "url": it["url"],
                            "name": it["name"],
                            "category": it["category"],
                            "color": it["color"],
                            "size": size,
                            "stock": it["stock"]
                        })
                else:
                    all_rows.append({
                        "url": it["url"],
                        "name": it["name"],
                        "category": it["category"],
                        "color": it["color"],
                        "size": "",
                        "stock": it["stock"]
                    })

            # маленькая пауза, чтобы не долбить сайт
            time.sleep(0.5)

            page += 1

        print(f"Нашёл карточек в каталоге (примерно): {total_found}")

    # сохраняем результат
    with open("output.csv", "w", newline="", encoding="utf-8") as f:
        fieldnames = ["url", "name", "category", "color", "size", "stock"]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(all_rows)

    print("\nГотово: output.csv")


if __name__ == "__main__":
    main()
