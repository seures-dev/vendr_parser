import threading
from queue import Queue
from src.producers import CategoryProducer
from src.http_client import HttpClient

urls = [
    "https://www.vendr.com/categories/devops",
    "https://www.vendr.com/categories/it-infrastructure",
    "https://www.vendr.com/categories/data-analytics-and-management"
    ]
stop_event = threading.Event()
http_client = HttpClient()
category_producer = CategoryProducer(
    http_client,
    urls, 
    Queue(),
    stop_event
    )
category_producer.produce()
