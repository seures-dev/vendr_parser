import os
import psycopg2
import time
from dotenv import load_dotenv
from queue import Empty
from multiprocessing import Queue
from dclass import Book

load_dotenv()

"""
docker run -d \
  --name vendr_postgres \
  -e POSTGRES_DB=vendr_db \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=your_password \
  -p 5432:5432 \
  -v pg_data:/var/lib/postgresql/data \
  postgres:15

"""


class Writer:
    def __init__(self, result_queue: Queue, task_queue: Queue):
        self.conn = psycopg2.connect(
            host="localhost",
            database=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            port=os.getenv("POSTGRES_PORT")
        )
        self.result_queue = result_queue
        self.task_queue = task_queue
        
    def run(self):
        self.create_table()
        time.sleep(15)
        while True:
            try:
                book: Book = self.result_queue.get(timeout=20)
                self.insert_book(book)
            except Empty:
                if not self.task_queue.empty():
                    continue
                print("Writer task queue is empty")
                break


            
            
    def create_table(self):
        """Create the books table if it doesn't exist."""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS books (
            id SERIAL PRIMARY KEY,
            book_title VARCHAR(255) NOT NULL,
            book_upc VARCHAR(255) NOT NULL,
            book_category VARCHAR(255) NOT NULL,
            book_description TEXT,
            book_price VARCHAR(255) NOT NULL,
            book_tax VARCHAR(255) NOT NULL,
            book_availability BOOLEAN NOT NULL,
            book_availability_count INTEGER NOT NULL,
            book_number_of_reviews INTEGER NOT NULL
        );
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(create_table_query)
                self.conn.commit()
                print("Table 'books' created or already exists.")
        except psycopg2.Error as e:
            print(f"Error creating table: {e}")
            self.conn.rollback()
            
    def insert_book(self, book: Book):
        """Insert a Book object into the books table."""
        insert_query = """
        INSERT INTO books (book_title, book_upc, book_category, book_description, book_price, book_tax, book_availability, book_availability_count, book_number_of_reviews)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(insert_query, (
                    book.book_title,
                    book.book_upc,
                    book.book_category,
                    book.book_description,
                    book.book_price,
                    book.book_tax,
                    book.book_availability,
                    book.book_availability_count,
                    book.book_number_of_reviews
                ))
                self.conn.commit()

        except psycopg2.Error as e:
            print(f"Error inserting book: {e}")
            self.conn.rollback()
            return None






