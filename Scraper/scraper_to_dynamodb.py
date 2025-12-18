import cloudscraper
from bs4 import BeautifulSoup
import boto3
import uuid
import datetime
import time

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('CarListings')

scraper = cloudscraper.create_scraper()

def save_car_to_aws(car_data):
    try:
        clean_data = {k: v for k, v in car_data.items() if v != ""}
        table.put_item(Item=clean_data)
        print(f" Saved to DB: {car_data.get('title', 'Unknown')} - {car_data.get('price', 'N/A')}")
    except Exception as e:
        print(f" Error saving: {e}")

def scrape_and_upload():
    url = "https://turbo.az/autos?sort=date"

    print(f"Starting scrape on {url}...")
    response = scraper.get(url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        products = soup.find('div', class_='products')

        if products:
            car_cards = products.find_all('div', class_='products-i')
            print(f"Found {len(car_cards)} cars. Uploading...")

            for card in car_cards:
                try:
                    link_tag = card.find('a', class_='products-i__link')
                    if not link_tag: continue

                    href = link_tag.get('href')
                    full_link = f"https://turbo.az{href}"
                    listing_id = href.split('/')[-1]

                    price_tag = card.find('div', class_='products-i__price')
                    price = price_tag.get_text(strip=True) if price_tag else "0"

                    attr_tag = card.find('div', class_='products-i__attributes')
                    attributes = attr_tag.get_text(strip=True) if attr_tag else "Unknown"

                    car_item = {
                        'listing_id': listing_id,
                        'make_model': 'Unknown',
                        'title': attributes,
                        'price': price,
                        'url': full_link,
                        'source': 'turbo.az',
                        'scraped_at': str(datetime.datetime.now())
                    }

                    save_car_to_aws(car_item)

                except Exception as e:
                    print(f"Skipped item: {e}")
        else:
            print("Could not find product list div.")
    else:
        print(f"Failed to fetch page: {response.status_code}")

if __name__ == "__main__":
    scrape_and_upload()