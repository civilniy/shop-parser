import requests
from bs4 import BeautifulSoup
import csv

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

def parse_product(url):
    response = requests.get(url, headers=HEADERS)
    soup = BeautifulSoup(response.text, "lxml")

    # Название
    name_tag = soup.find("h1")
    name = name_tag.get_text(strip=True) if name_tag else ""

    # Категория (часто в breadcrumbs)
    category = ""
    breadcrumbs = soup.select("nav a")
    if breadcrumbs:
        category = " / ".join([b.get_text(strip=True) for b in breadcrumbs][-3:])

    # Цвет
    color = ""
    color_tag = soup.find(string=lambda t: "цвет" in t.lower() if t else False)
    if color_tag:
        color = color_tag.strip()

    # Размер
    size = ""
    size_tag = soup.find(string=lambda t: "размер" in t.lower() if t else False)
    if size_tag:
        size = size_tag.strip()

    # Остаток
    stock = ""
    page_text = soup.get_text(" ", strip=True).lower()
    if "нет в наличии" in page_text:
        stock = "нет в наличии"
    elif "в наличии" in page_text:
        stock = "в наличии"

    return {
        "url": url,
        "name": name,
        "category": category,
        "color": color,
        "size": size,
        "stock": stock
    }


def main():
    with open("urls.txt", "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip()]

    results = []

    for url in urls:
        print(f"Парсим {url}")
        product = parse_product(url)
        results.append(product)

    with open("output.csv", "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = ["url", "name", "category", "color", "size", "stock"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    print("Готово! Файл output.csv создан.")


if __name__ == "__main__":
    main()
