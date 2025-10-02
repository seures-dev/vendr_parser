import threading

from queue import Queue, Empty
from .http_client import HttpClient
from .parser import ProductParser
from .logger import get_logger


class ProductWorker(threading.Thread):
    """
    Worker that takes product URLs from task_queue, fetches pages,
    parses them and pushes Product into write_queue.
    """

    def __init__(
        self,
        http_client: HttpClient,
        parser: ProductParser,
        task_queue: Queue,
        write_queue: Queue,
        stop_event: threading.Event = None
    ):
        super().__init__(daemon=True)
        self.http_client = http_client
        self.parser = parser
        self.task_queue = task_queue
        self.write_queue = write_queue
        self.stop_event = stop_event or threading.Event()
        self.logger = get_logger("ProductWorker")

    def run(self):
        while not (self.stop_event.is_set() and self.task_queue.empty()):
            try:
                task = self.task_queue.get(timeout=0.5)  # task is tuple (url, category_hint)
            except Empty:
                continue
            try:
                url, category_hint = task
                html_text = self.http_client.fetch(url)
                if not html_text:
                    self.logger.debug("Empty html for %s", url)
                else:
                    product = self.parser.parse_product_page(html_text, url, category_hint)
                    if product:
                        self.write_queue.put(product)
                        self.logger.info("Parsed product: %s", product.name)
                    else:
                        self.logger.debug("Parser returned None for %s", url)
            except Exception as e:
                self.logger.exception("Error processing task %s: %s", task, e)
            finally:
                self.task_queue.task_done()
