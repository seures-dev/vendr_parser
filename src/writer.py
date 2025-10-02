import os
from typing import List
import threading
from queue import Empty, Queue
import psycopg2
from psycopg2.extras import execute_values
from src.logger import get_logger
from src.product import Product


DB_WRITER_BATCH = int(os.environ.get("DB_WRITER_BATCH", "20"))


class PostgresWriter(threading.Thread):
    """
    Dedicated DB writer thread which consumes Product items from a queue and writes them to Postgres.
    Uses batched insert for efficiency.
    """

    def __init__(self, dsn: str, write_queue: Queue,
                 batch_size: int = DB_WRITER_BATCH,
                 stop_event: threading.Event = None):
        super().__init__(name="DBWriter", daemon=True)
        self.dsn = dsn
        self.queue = write_queue
        self.batch_size = batch_size
        self.stop_event = stop_event or threading.Event()
        self._buffer: List[Product] = []
        self._conn = None
        self.logger = get_logger("PostgresWriter")

    def run(self):
        self.logger.info("DB writer starting.")
        try:
            self._conn = psycopg2.connect(self.dsn)
            self._ensure_table()
            while not (self.stop_event.is_set() and self.queue.empty()):
                try:
                    item: Product = self.queue.get(timeout=0.5)
                    self._buffer.append(item)
                    if len(self._buffer) >= self.batch_size:
                        self._flush()
                    self.queue.task_done()
                except Empty:
                    if self._buffer:  # flush periodically
                        self._flush()
                except Exception as e:
                    self.logger.exception("Unexpected error in DB writer loop: %s", e)

            # final flush
            if self._buffer:
                self._flush()
        except Exception as ex:
            self.logger.exception("Unexpected error in DB writer loop: %s", ex)
        finally:
            if self._conn:
                self._conn.close()
            self.logger.info("DB writer stopped.")

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

    def _flush(self):
        if not self._buffer:
            return

        # delete duplicate by name + category key
        seen = set()
        unique_rows = []
        for p in self._buffer:
            key = (p.name, p.category)
            if key not in seen:
                seen.add(key)
                unique_rows.append(p)

        rows = [p.as_tuple() for p in unique_rows]
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
        self._buffer.clear()
