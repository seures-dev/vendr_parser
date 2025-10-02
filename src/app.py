import threading
from queue import Queue
from typing import List
from src.http_client import HttpClient
from src.parser import ProductParser
from src.producers import CategoryProducer
from src.worker import ProductWorker
from src.writer import PostgresWriter
from src.logger import get_logger


class ScraperApp:
    """Orchestrates producer, workers, and DB writer threads."""

    def __init__(self, category_urls: List[str], worker_count: int, postgre_dsn: str):
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
        self.db_writer = PostgresWriter(postgre_dsn, self.write_queue, stop_event=self.stop_event)
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
            self.write_queue.join()  # note: we don't call task_done on write_queue; instead DB writer drains it
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
