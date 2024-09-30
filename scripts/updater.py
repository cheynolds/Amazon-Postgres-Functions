import psycopg2
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import os
import traceback
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

# PostgreSQL connection parameters
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

def connect_to_db():
    """Establishes connection to PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        cursor = conn.cursor()
        return conn, cursor
    except Exception as e:
        print(f"Failed to connect to PostgreSQL: {e}")
        return None, None

def fetch_all_products(cursor):
    """Fetch all products from product_data table."""
    query = "SELECT asin, product_link, price, reviews FROM product_data"
    cursor.execute(query)
    return cursor.fetchall()

def scrape_product_data(url):
    """Scrape product data from Amazon URL (price and reviews)."""
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.content, 'html.parser')

        # Scrape price
        price_element = soup.select_one('span.a-price > span.a-offscreen')
        price = price_element.text.strip().replace('$', '').replace(',', '') if price_element else None

        # Scrape reviews
        reviews_element = soup.select_one('a[id^="review_count"]')
        reviews = reviews_element.text.strip().replace(',', '') if reviews_element else None

        return float(price), int(reviews) if price and reviews else (None, None)
    except Exception as e:
        print(f"Failed to scrape data from {url}: {e}")
        return None, None

def update_product_in_postgres(cursor, conn, product_data, new_price, new_reviews):
    """Update product data and log history if price or reviews changed."""
    try:
        asin, old_price, old_reviews = product_data['asin'], product_data['price'], product_data['reviews']

        if new_price != old_price or new_reviews != old_reviews:
            # Insert old data into history
            cursor.execute(
                """
                INSERT INTO product_data_history (asin, price, reviews, updated_at)
                VALUES (%s, %s, %s, %s)
                """,
                (asin, old_price, old_reviews, datetime.now())
            )

            # Update current data
            cursor.execute(
                """
                UPDATE product_data
                SET price = %s, reviews = %s
                WHERE asin = %s
                """,
                (new_price, new_reviews, asin)
            )
            conn.commit()
            print(f"Updated product {asin}: price {old_price} -> {new_price}, reviews {old_reviews} -> {new_reviews}")
        else:
            print(f"No changes for product {asin}")
    except Exception as e:
        print(f"Failed to update product {asin}: {e}")
        conn.rollback()

def main():
    # Connect to PostgreSQL
    conn, cursor = connect_to_db()
    if not conn:
        return

    # Fetch all products
    products = fetch_all_products(cursor)

    # Loop through each product and update data
    for product in products:
        asin, url, old_price, old_reviews = product
        new_price, new_reviews = scrape_product_data(url)

        if new_price is not None and new_reviews is not None:
            product_data = {'asin': asin, 'price': old_price, 'reviews': old_reviews}
            update_product_in_postgres(cursor, conn, product_data, new_price, new_reviews)

    # Close connection
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
