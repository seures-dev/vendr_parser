
from typing import List
import threading
from queue import Queue
import psycopg2
from psycopg2.extras import execute_values
from src.product import Product
from src.databases.awriter import AWriter


class PostgresWriter(AWriter):
    """
    Dedicated DB writer thread which consumes Product items from a queue and writes them to Postgres.
    Uses batched insert for efficiency.
    """

    def __init__(
        self, dsn: str,
        write_queue: Queue,
        batch_size: int = 20,
        stop_event: threading.Event = None
    ):
        super().__init__(write_queue, batch_size, stop_event, name="PostgresWriter")
        self.dsn = dsn
        self._conn = None

    def run(self):
        # connect to database and run loop
        self._conn = psycopg2.connect(self.dsn)
        self._ensure_table()
        super().run()
        if self._conn:
            self._conn.close()

    def _write(self, items: List[Product]):
        rows = [p.as_tuple() for p in items]
        with self._conn.cursor() as cur:
            query = """
            INSERT INTO vendr_products (name, description, category, min_price, max_price, median_price)
            VALUES %s
            ON CONFLICT (name, category) DO UPDATE SET
                description = EXCLUDED.description,
                min_price = EXCLUDED.min_price,
                max_price = EXCLUDED.max_price,
                median_price = EXCLUDED.median_price,
                scraped_at = now()
            ;
            """
            execute_values(cur, query, rows)
            self._conn.commit()
        self.logger.info("Wrote %d products to DB.", len(rows))

    def _get_unique_key(self, item: Product):
        return (item.name, item.category)

    def _ensure_table(self):
        with self._conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS vendr_products (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT,
                    category TEXT,
                    min_price INTEGER,
                    max_price INTEGER,
                    median_price INTEGER,
                    scraped_at TIMESTAMP DEFAULT now(),
                    CONSTRAINT unique_product UNIQUE (name, category)
                );
                """
            )
            self._conn.commit()
        self.logger.debug("Ensured vendr_products table exists.")
