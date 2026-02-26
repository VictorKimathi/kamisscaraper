import requests
import json
import time
from bs4 import BeautifulSoup

BASE_URL = "https://kamis.kilimo.go.ke/site/market"

def get_product_list():
    """Fetch the main page and extract product options from the dropdown."""
    resp = requests.get(BASE_URL)
    soup = BeautifulSoup(resp.text, 'html.parser')
    select = soup.find('select', {'name': 'product'})
    if not select:
        print("Could not find product dropdown.")
        return []
    options = select.find_all('option')
    products = []
    for opt in options:
        value = opt.get('value')
        if value and value.isdigit():  # skip the placeholder
            products.append({
                'id': value,
                'name': opt.text.strip()
            })
    return products

def scrape_product(product_id, product_name, per_page=10):
    """Fetch data for a single product and return list of records."""
    url = f"{BASE_URL}?product={product_id}&per_page={per_page}"
    print(f"Fetching: {product_name} (ID: {product_id})")
    try:
        resp = requests.get(url)
        soup = BeautifulSoup(resp.text, 'html.parser')
        table = soup.find('table', class_='table table-bordered table-condensed')
        if not table:
            print(f"  No table found for {product_name}")
            return []

        headers = []
        thead = table.find('thead')
        if thead:
            headers = [th.get_text(strip=True) for th in thead.find_all('th')]

        tbody = table.find('tbody')
        if not tbody:
            return []

        rows = tbody.find_all('tr')
        records = []
        for row in rows:
            cells = row.find_all('td')
            if len(cells) == len(headers):
                row_data = {'Product': product_name}  # add product name
                for i, cell in enumerate(cells):
                    text = cell.get_text(strip=True)
                    row_data[headers[i]] = text if text not in ('', '-') else None
                records.append(row_data)
        return records
    except Exception as e:
        print(f"  Error scraping {product_name}: {e}")
        return []

def main():
    products = get_product_list()
    print(f"Found {len(products)} products.")
    all_data = []

    for prod in products:
        data = scrape_product(prod['id'], prod['name'])
        all_data.extend(data)
        time.sleep(1)  # be polite to the server

    with open('all_commodities.json', 'w', encoding='utf-8') as f:
        json.dump(all_data, f, indent=2, ensure_ascii=False)

    print(f"Done. Total records: {len(all_data)}")

if __name__ == "__main__":
    main()