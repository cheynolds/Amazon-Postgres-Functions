import psycopg2
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import os
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import re

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
    """Fetch all products from product_data table based on oldest or null last_CheckDate."""
    query = """
    SELECT asin, product_link, price, reviews, last_CheckDate
    FROM product_data
    WHERE last_CheckDate IS NULL OR last_CheckDate = (
        SELECT MIN(last_CheckDate) FROM product_data
    )
    """
    cursor.execute(query)
    return cursor.fetchall()


def scrape_product_data(url):
    """Scrape product data from Amazon URL (price, reviews, and stars) using Selenium and BeautifulSoup."""
    try:
        # Initialize Selenium WebDriver
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')  # Run in headless mode to avoid opening a browser window
        driver = webdriver.Chrome(options=options)
        driver.get(url)

        # Wait for the page to load
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'span.a-price-whole, span#acrCustomerReviewText'))
            )
        except TimeoutException:
            print(f"Page did not load fully for URL: {url}")
            driver.quit()
            return 0.0, 0, 0.0  # Default values on failure

        # Use BeautifulSoup to parse the page source
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        price = 0.00
        reviews = 0
        stars = 0.00

        # Scrape price using BeautifulSoup
        try:
           price_whole_element = soup.select_one('span.a-price-whole')
           price_fraction_element = soup.select_one('span.a-price-fraction')

           # Extract text for whole and fraction parts, ensuring they exist
           price_whole = price_whole_element.text.strip().replace(',', '') if price_whole_element else "0"
           price_fraction = price_fraction_element.text.strip() if price_fraction_element else "00"

           # Ensure the fractional part is properly formatted (i.e., contains only 2 digits)
           if len(price_fraction) > 2:
               price_fraction = price_fraction[:2]  # Limit to 2 digits for safety

           # Safely concatenate the whole and fractional parts to avoid double periods
           if not price_whole.endswith('.'):
               price = f"{price_whole}.{price_fraction}"
           else:
               price = f"{price_whole}{price_fraction}"

           print(f"Raw Price Found: {price}")

           # Convert the price string to a float
           price = float(price)
           print(f"Price extracted: {price}")

        except Exception as e:
           print(f"Failed to extract price: {e}")
           price = 0.0

        # Scrape reviews
        try:
            reviews_element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'span#acrCustomerReviewText'))
            )
            reviews = reviews_element.text.strip().split()[0].replace(',', '')
            reviews = int(reviews) if reviews.isdigit() else 0
            print(f"Reviews found: {reviews}")
        except TimeoutException:
            print(f"Failed to locate reviews element: {url}")
            reviews = 0

        # Scrape stars
        try:
            stars_element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'i.a-icon-star span.a-icon-alt'))
            )
            stars_text = stars_element.get_attribute('textContent').strip()
            if stars_text and "out of" in stars_text:
                stars = float(stars_text.split()[0])
            print(f"Stars found: {stars}")
        except TimeoutException:
            print(f"Failed to locate stars element: {url}")
            stars = 0.0

        # Close the browser
        driver.quit()

        # Return the scraped values
        return price, reviews, stars

    except Exception as e:
        print(f"Failed to scrape data from {url}: {e}")
        return 0.0, 0, 0.0  # Return default values on error

def update_product_in_postgres(cursor, conn, product_data, new_price, new_reviews, new_stars):
    """Update product data and log history if price, reviews, or stars changed."""
    try:
        asin, old_price, old_reviews, old_check_date = product_data['asin'], product_data['price'], product_data['reviews'], product_data['last_CheckDate']
        old_stars = product_data.get('stars', 0.0)  # Default to 0.0 if not available

        # Get the current date
        current_date = datetime.now().date()

        # Calculate price change and percentage change if there's a change in price
        last_PriceChange = round(new_price - old_price, 2) if old_price else 0.0
        last_PriceChange_Percent = round(((new_price - old_price) / old_price) * 100, 2) if old_price else 0.0

        # Log history if there are changes in price, reviews, or stars
        if new_price != old_price or new_reviews != old_reviews or new_stars != old_stars:
            # Insert old data into history
            cursor.execute(
                """
                INSERT INTO product_data_history (asin, price, reviews, stars, updated_at)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (asin, old_price, old_reviews, old_stars, datetime.now())
            )

            # Update current data, ensuring reviews and stars are valid
            cursor.execute(
                """
                UPDATE product_data
                SET price = %s, reviews = %s, stars = %s, last_CheckDate = %s, 
                    last_PriceChange = %s, last_PriceChange_Percent = %s
                WHERE asin = %s
                """,
                (new_price, new_reviews, new_stars, current_date, last_PriceChange, last_PriceChange_Percent, asin)
            )
            conn.commit()

            print(f"Updated product {asin}: price {old_price} -> {new_price}, reviews {old_reviews} -> {new_reviews}, stars {old_stars} -> {new_stars}, price change: {last_PriceChange}, price change percent: {last_PriceChange_Percent}%")
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
        new_price, new_reviews, new_stars = scrape_product_data(url)

        if new_price != 0.0 or new_reviews != 0:
            product_data = {'asin': asin, 'price': old_price, 'reviews': old_reviews}
            update_product_in_postgres(cursor, conn, product_data, new_price, new_reviews, new_stars)

    # Close connection
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
