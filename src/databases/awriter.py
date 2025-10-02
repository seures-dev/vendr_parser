from typing import List
import threading
from queue import Empty, Queue
from abc import ABC, abstractmethod
from src.logger import get_logger
from src.product import Product


class AWriter(ABC, threading.Thread):
    """
    Abstract writer pattern for realization of different output mechanisms.

    This class serves as a base for concrete writer implementations.
    Subclasses should implement the `_write` method to define how data
    is actually written, e.g., to a database, file, or external service.

    Provides:
    - Standardized interface for writing data from a queue
    - Optional buffering or batching mechanisms
    - Error handling and logging support
    """

    def __init__(
        self, write_queue: Queue,
        batch_size: int = 20,
        stop_event: threading.Event = None,
        name: str = "Writer"
    ):
        super().__init__(name=name, daemon=True)
        self.queue = write_queue
        self.batch_size = batch_size
        self.stop_event = stop_event or threading.Event()
        self._buffer: List[Product] = []
        self.logger = get_logger(name)

    def run(self):
        """Main writer loop. Check queue for new products and add to _buffer
        when _buffer size more or eq batch_size size call _flush for write"""
        self.logger.info(f"{self.name} starting.")
        try:
            while not (self.stop_event.is_set() and self.queue.empty()):
                try:
                    item: Product = self.queue.get(timeout=0.5)
                    self._buffer.append(item)
                    if len(self._buffer) >= self.batch_size:
                        self._flush()
                    self.queue.task_done()
                except Empty:
                    if self._buffer:
                        self._flush()
                except Exception as e:
                    self.logger.exception("Unexpected error in writer loop: %s", e)

            # final flush
            if self._buffer:
                self._flush()
        finally:
            self.logger.info(f"{self.name} stopped.")

    def _flush(self):
        if not self._buffer:
            return

        # delete duplicate by key
        seen = set()
        unique_rows = []
        for p in self._buffer:
            key = self._get_unique_key(p)
            if key not in seen:
                seen.add(key)
                unique_rows.append(p)

        self._write(unique_rows)
        self._buffer.clear()

    @abstractmethod
    def _write(self, items: List[Product]):
        """
        This method writes data to your database using the appropriate library.
        """
        pass

    @abstractmethod
    def _get_unique_key(self, item: Product):
        """
        Return unique key for filter duplicate in buffer
        """
        pass
