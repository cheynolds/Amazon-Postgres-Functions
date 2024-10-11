import psycopg2
from dotenv import load_dotenv
import os

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

def main():

    # Connect to PostgreSQL
    conn, cursor = connect_to_db()
    if not conn:
        return

    # Query to delete records where stars = 0.0
    delete_query = """
    DELETE FROM product_data
    WHERE stars = 0.0;
    """

    # Execute the delete query
    cursor.execute(delete_query)

    # Commit the transaction
    conn.commit()

    # Print the number of deleted rows
    print(f"Deleted {cursor.rowcount} rows where stars = 0.0.")

    # Close the connection
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()