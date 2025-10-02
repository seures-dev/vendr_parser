from pathlib import Path
import threading
from queue import Queue
from typing import List
from src.http_client import HttpClient
from src.parser import ProductParser
from src.producers import CategoryProducer
from src.worker import ProductWorker
import src.databases as databases
from src.logger import get_logger


class ScraperApp:
    """Orchestrates producer, workers, and DB writer threads."""

    def __init__(self, category_urls: List[str], worker_count: int):
        self.http_client = HttpClient()
        self.category_urls = category_urls
        self.parser = ProductParser()
        self.task_queue: Queue = Queue()
        self.write_queue: Queue = Queue()
        self.stop_event = threading.Event()
        self.producer = CategoryProducer(
            self.http_client,
            self.category_urls,
            self.task_queue,
            stop_event=self.stop_event
        )
        self.workers: List[ProductWorker] = [
            ProductWorker(self.http_client, self.parser, self.task_queue, self.write_queue, stop_event=self.stop_event)
            for _ in range(worker_count)
        ]
        database_dsn = databases.get_db_dsn()
        banch_size = databases.get_writer_batch()

        # if string database_dsn contains "postgresql" create writer to work with PostgreSQL otherwise SQLite
        if "postgresql" in database_dsn:
            self.db_writer: databases.AWriter = databases.PostgresWriter(
                database_dsn, self.write_queue, banch_size, stop_event=self.stop_event
            )
        else:
            dsn_path = Path(database_dsn)

            # if parent dir for database in dsn_path not exist or it's not a dir create default path
            if not (dsn_path.parent.exists() and dsn_path.parent.is_dir()):
                self.logger.exception("Wrong dsn_path(%s), generage default dsn_path")
                database_dsn = databases.get_db_dsn(use_env=False)

            self.db_writer: databases.AWriter = databases.SqliteWriter(
                database_dsn, self.write_queue, banch_size, stop_event=self.stop_event
            )

        self.logger = get_logger("ScraperApp")

    def start(self):
        self.logger.info("ScraperApp starting.")
        # start DB writer
        self.db_writer.start()
        # start worker threads
        for w in self.workers:
            w.start()
        # producer runs in main thread here (could be a separate thread if desired)
        try:
            self.producer.produce()
            # Wait for all enqueued tasks to be processed
            self.logger.info("Producer done. Waiting for task queue to drain...")
            self.task_queue.join()
            self.logger.info("Task queue drained. Waiting for write queue to finish...")
            self.write_queue.join() 
            # signal writer to flush and stop
            self.stop_event.set()
            # Wait for writer to finish
            self.db_writer.join(timeout=30)
        except KeyboardInterrupt:
            self.logger.info("Interrupted by user, shutting down...")
            self.stop_event.set()
        finally:
            # ensure stop event set
            self.stop_event.set()
            self.logger.info("ScraperApp finished.")
