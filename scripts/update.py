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
import random
from datetime import timedelta

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









def fetch_all_products(cursor, limit=1000):
    """Fetch all products from product_data table ordered by oldest last_checkdate or NULL."""
    query = """
    SELECT asin, product_link, price, reviews, stars, last_checkdate
    FROM product_data
    ORDER BY last_checkdate ASC NULLS FIRST
    """
    cursor.execute(query, (limit,))
    return cursor.fetchall()

    #ORDER BY last_checkdate ASC NULLS FIRST
    #LIMIT %s
    




def initialize_driver(headless=False, max_retries=3):
    """Initialize Selenium WebDriver with retries."""
    retries = 0
    while retries < max_retries:
        try:
            options = webdriver.ChromeOptions()
            if headless:
                options.add_argument('--headless')
            driver = webdriver.Chrome(options=options)
            return driver
        except WebDriverException as e:
            print(f"Failed to initialize WebDriver: {e}. Retrying {retries+1}/{max_retries}...")
            retries += 1
            time.sleep(3)  # Wait before retrying
    return None








def scrape_product_data(driver, url, max_retries=3):
    """Scrape product data from Amazon URL (price, reviews, and stars) using Selenium and BeautifulSoup."""
    retries = 0
    while retries < max_retries:
        try:
            # Initialize Selenium WebDriver
            # options = webdriver.ChromeOptions()
            # options.add_argument('--headless')  # Run in headless mode to avoid opening a browser window
            # driver = webdriver.Chrome(options=options)
            driver.get(url)

            # Check if CAPTCHA is detected
            if "captcha" in driver.page_source.lower():
                print(f"CAPTCHA detected for URL: {url}")
                # Switch to non-headless mode to solve the CAPTCHA manually
                driver = solve_captcha(driver, url)



            # Wait for the page to load
            try:
                WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'span.a-price-whole, span#acrCustomerReviewText'))
                )
            except TimeoutException:
                print(f"Page did not load fully for URL: {url}. Skipping this product.")
                #return 0.0, 0, 0.0  # Default values on failure
                return None  # Return None to indicate failure
                
            # except TimeoutException:
            #     print(f"Page did not load fully for URL: {url}")
            #     driver.quit()
            #     return 0.0, 0, 0.0  # Default values on failure

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

               #print(f"Raw Price Found: {price}")

               # Convert the price string to a float
               price = float(price)
               #print(f"Price extracted: {price}")

            except Exception as e:
               print(f"Failed to extract price: {e}")
               price = None
               time.sleep(5)  # Small delay before retrying to avoid overwhelming the server

            # Scrape reviews
            try:
                reviews_element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'span#acrCustomerReviewText'))
                )
                reviews = reviews_element.text.strip().split()[0].replace(',', '')
                reviews = int(reviews) if reviews.isdigit() else 0
                #print(f"Reviews found: {reviews}")
            except TimeoutException:
                print(f"Failed to locate reviews element: {url}")
                reviews = None

            # Scrape stars
            try:
                stars_element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'i.a-icon-star span.a-icon-alt'))
                )
                stars_text = stars_element.get_attribute('textContent').strip()
                if stars_text and "out of" in stars_text:
                    stars = float(stars_text.split()[0])
                #print(f"Stars found: {stars}")
            except TimeoutException:
                print(f"Failed to locate stars element: {url}")
                stars = None

            # Close the browser
            #driver.quit()

            # Return the scraped values
            return price, reviews, stars

        except Exception as e:
            print(f"Failed to scrape data from {url}: {e}")
            return None  # Return default values on error
    # If retries exceed max_retries, log and return default values
    print(f"Failed to load page after {max_retries} retries for URL: {url}")
    return None  # Return default values on error










def solve_captcha(driver, url):
    """Switch out of headless mode, pause the script for manual CAPTCHA solving, and continue."""
    print(f"Please solve the CAPTCHA manually for {url}.")
    input("Press Enter once you've solved the CAPTCHA.")
#    return driver









def update_product_in_postgres(cursor, conn, product_data, new_price, new_reviews, new_stars):
    """Update product data and log history if price, reviews, or stars changed."""
    try:
        asin, old_price, old_reviews, old_check_date = product_data['asin'], product_data['price'], product_data['reviews'], product_data['last_checkdate']
        old_stars = product_data.get('stars', 0.0)  # Default to 0.0 if not available

        # Get the current date
        current_date = datetime.now().date()

        # Calculate price change and percentage change if there's a change in price
        last_PriceChange = round(new_price - old_price, 2) if old_price else 0.0
        last_PriceChange_Percent = round(((new_price - old_price) / old_price) * 100, 2) if old_price else 0.0

        # Log history if there are changes in price, reviews, or stars
        if new_price != old_price or new_reviews != old_reviews or new_stars != old_stars or current_date != old_check_date:
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
                SET price = %s, reviews = %s, stars = %s, last_checkdate = %s, 
                    last_pricechange = %s, last_pricechange_percent = %s
                WHERE asin = %s
                """,
                (new_price, new_reviews, new_stars, current_date, last_PriceChange, last_PriceChange_Percent, asin)
            )
            conn.commit()

            # Return the price change and percentage for printing later
            return last_PriceChange, last_PriceChange_Percent
        else:
            print(f"No changes for product {asin}")
            return None, None  # Return None if no changes were made
    except Exception as e:
        print(f"Failed to update product {asin}: {e}")
        conn.rollback()
        return None, None

def main():
    # Record the overall start time
    overall_start_time = time.time()

    driver = initialize_driver()

    # Connect to PostgreSQL
    conn, cursor = connect_to_db()
    if not conn:
        return

    # Fetch all products based on the oldest or null last_CheckDate
    products = fetch_all_products(cursor)
    total_products = len(products)  # Get the total number of products

    # Keep track of completed records and elapsed time per record
    total_time_spent = 0
    completed_records = 0

    # Loop through each product and update data
    for product in products:
        asin, url, old_price, old_reviews, old_stars, last_check_date = product
    
        # Processing message for each product
        print(f"\nProcessing product {asin} last checked {last_check_date}")
        #print(f"Old stars: {old_stars}")  # Display old stars for reference

        # Record the start time for this record
        record_start_time = time.time()

        # Scrape new data from the Amazon page
        scraped_data = scrape_product_data(driver, url)

        # If scraping failed, skip to the next product
        if scraped_data is None:
            print(f"Skipping product {asin} due to scraping mismatch.")
            continue  # Skip to the next product

        new_price, new_reviews, new_stars = scraped_data
        # Output the scraped data for debugging
        #print(f"Raw Price Found: {new_price}")
        print(f"Price extracted: {new_price}")
        print(f"Reviews found: {new_reviews}")
        print(f"Stars found: {new_stars}")
        print(f"Old stars: {old_stars}")

        # Check if any updates are required
        if new_price != 0.0 or new_reviews != 0 or new_stars != old_stars:
            product_data = {
                'asin': asin,
                'price': old_price,
                'reviews': old_reviews,
                'stars': old_stars,
                'last_checkdate': last_check_date
            }

            # Update the product in the database with new price, reviews, and stars
            last_PriceChange, last_PriceChange_Percent = update_product_in_postgres(cursor, conn, product_data, new_price, new_reviews, new_stars)
            
            # Print the updated product information if updates occurred
            if last_PriceChange is not None and last_PriceChange_Percent is not None:
                print(f"Updated product {asin}: price {old_price} -> {new_price}, reviews {old_reviews} -> {new_reviews}, stars {old_stars} -> {new_stars}, price change: {last_PriceChange}, price change percent: {last_PriceChange_Percent}%")
        else:
            print(f"No changes were recorded for product {asin}")
    
        # Calculate time taken for this record
        time_spent = time.time() - record_start_time
        total_time_spent += time_spent
        completed_records += 1

        # Estimate remaining time
        avg_time_per_record = total_time_spent / completed_records
        remaining_records = total_products - completed_records
        estimated_time_remaining = avg_time_per_record * remaining_records

        # Convert time to a human-readable format
        readable_remaining_time = str(timedelta(seconds=estimated_time_remaining))

        # Print progress with a line break
        print(f"\n{'-'*30}\nProgress: {completed_records}/{total_products}, Estimated Time Remaining: {readable_remaining_time}\n{'-'*30}")
    
        # Add a random delay between 2 and 5 seconds to simulate human behavior and avoid rate limits
        time.sleep(random.uniform(2, 5))

        
    # Close connection
    cursor.close()
    conn.close()

    # Quit the WebDriver after all products are scraped
    driver.quit()

    # Calculate and display the total elapsed time
    total_elapsed_time = time.time() - overall_start_time
    readable_total_time = str(timedelta(seconds=total_elapsed_time))
    print(f"\nScript finished. Total run time: {readable_total_time}")

if __name__ == "__main__":
    main()
