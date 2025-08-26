from pathlib import Path
import json
import re
import time
from urllib.parse import urlparse, urlunparse
import requests
from lxml import html
from queue import Queue
from threading import Thread
from src.writer import Writer
from src.parser import Parser, Task, TaskType, ProductDetails


def monitoring(parser_task_queue: Queue, parser_result_queue: Queue):
    empty_count = 0
    while True:
        time.sleep(5)
        print(f"Parser task queue size: {parser_task_queue.qsize()}")
        print(f"Parser result queue size: {parser_result_queue.qsize()}")
        if parser_result_queue.empty() and parser_task_queue.empty():
            empty_count += 1
            if empty_count > 2:
                break
        else:
            empty_count = 0
    
    
def main():
    parser_count = 30
    user_agents = json.load(open("src/user-agents.json"))
    parser_task_queue = Queue()
    parser_result_queue = Queue()
    parser_task_queue.put(Task(TaskType.EXTRACT_SUBCATEGORIES, "https://www.vendr.com/categories/devops", "https://www.google.com/"))
    parser_task_queue.put(Task(TaskType.EXTRACT_SUBCATEGORIES, "https://www.vendr.com/categories/it-infrastructure", "https://www.google.com/"))
    parser_task_queue.put(Task(TaskType.EXTRACT_SUBCATEGORIES, "https://www.vendr.com/categories/data-analytics-and-management", "https://www.google.com/"))
    for i in range(parser_count):
        ua_index = i % len(user_agents)
        parser = Parser(parser_task_queue, parser_result_queue, user_agents[ua_index])
        parser_thread = Thread(target=parser.run)
        parser_thread.start()
        time.sleep(0.5)

    writer = Writer(parser_result_queue, parser_task_queue)
    writer_thread = Thread(target=writer.run)
    writer_thread.start()
    monitoring_thread = Thread(target=monitoring, args=(parser_task_queue, parser_result_queue))
    monitoring_thread.start()



if __name__ == "__main__":
    main()