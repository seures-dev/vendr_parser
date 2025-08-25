import os
import psycopg2
from dotenv import load_dotenv
from queue import Empty, Queue
import time
from .parser import ProductDetails

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
                product: ProductDetails = self.result_queue.get(timeout=20)
                self.insert_product(product)
            except Empty:
                if not self.task_queue.empty():
                    continue
                print("Writer task queue is empty")
                break


            
            
    def create_table(self):
        """Create the products table if it doesn't exist."""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS products (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            description TEXT,
            min_price INTEGER,
            max_price INTEGER,
            median_price INTEGER
        );
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(create_table_query)
                self.conn.commit()
                print("Table 'products' created or already exists.")
        except psycopg2.Error as e:
            print(f"Error creating table: {e}")
            self.conn.rollback()
            
    def insert_product(self, product: ProductDetails):
        """Insert a ProductDetails object into the products table."""
        insert_query = """
        INSERT INTO products (name, description, min_price, max_price, median_price)
        VALUES (%s, %s, %s, %s, %s)
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(insert_query, (
                    product.name,
                    product.description,
                    product.min_price,
                    product.max_price,
                    product.median_price
                ))
                self.conn.commit()
            self.result_queue.task_done()
                # print(f"Inserted product '{product.name}'")
        except psycopg2.Error as e:
            print(f"Error inserting product: {e}")
            self.conn.rollback()
            return None






