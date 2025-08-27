import os
import re
from urllib.parse import urljoin
from playwright.async_api import async_playwright
import asyncio
from multiprocessing import Queue
from queue import Empty
from dclass import Task, Error, Book, TaskType

from random import randint 

from dotenv import load_dotenv
load_dotenv()

ERROR_RATE = int(os.getenv("RAISE_ERROR_CHANCE", "-1"))



class Scraper:
    def __init__(self, scraper_id: int, task_queue: Queue, error_queue: Queue, result_queue: Queue):
        self.playwright = None
        self.page = None
        self.browser = None
        self.task_queue = task_queue
        self.error_queue = error_queue
        self.result_queue = result_queue
        self.scraper_id = scraper_id
        self.current_task = None
        self.running = False
        

    async def start(self):
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=False)
        self.page = await self.browser.new_page()
        self.page.set_default_timeout(5000)
        self.page.set_default_navigation_timeout(10000)
        self.running = True


    def run(self):
        asyncio.run(self._run())

    async def _run(self):
        empty_count = 0
        await self.start()
        await asyncio.sleep(0.5)
        while True:
            if not self.running:
                break
            try:
                task = self.task_queue.get(timeout=3)
                self.current_task = task
                empty_count = 0
            except Empty:
                empty_count += 1
                if empty_count > 3:
                    if self.current_task:
                        self.task_queue.put(self.current_task)
                        self.current_task = None
                    break
                continue
            if task.task_type == TaskType.ScrapeBookLinks:
                await self.scrape_book_links(task.url)
            elif task.task_type == TaskType.ScrapeBook:
                await self.scrape_book(task.url)
                
        await self.close()

    async def scrape_book_links(self, url: str):
        if url == "https://books.toscrape.com/index.html":
            base_url = "https://books.toscrape.com/catalogue/category/books_1/"
            for i in range(2, 51):
                page_url = f"{base_url}page-{i}.html"
                task = Task(page_url, TaskType.ScrapeBookLinks)
                self.task_queue.put(task)
        try:
            await self.page.goto(url, wait_until="domcontentloaded")
            is_error = randint(0, 100) < ERROR_RATE - 1
            if is_error:
                raise TimeoutError("TimeoutError")


            links = self.page.locator(".product_pod h3 a")
            links_count = await links.count()
            
            for i in range(links_count):
                link = links.nth(i)
                book_href = await link.get_attribute("href")
                book_url = urljoin(self.page.url, book_href)
                task = Task(book_url, TaskType.ScrapeBook)
                self.task_queue.put(task)
        except Exception:
            error = Error(self.current_task.url, TaskType.ScrapeBookLinks, "Error while scraping book links", self.scraper_id)
            self.error_queue.put(error)
            await asyncio.sleep(1)
            await self.close()
            return

    async def close(self):
        self.running = False
        await self.browser.close()
        await self.playwright.stop()

    async def scrape_book(self, book_url: str):
        await self.page.goto(book_url, wait_until="domcontentloaded")
        try:
            book_title = await self.page.text_content(".product_main h1")
            book_category = await self.page.locator(".breadcrumb li a").last.text_content()
            description_count = await self.page.locator(".product_page p:not([class])").count()
            image_src = await self.page.locator(".item.active img").get_attribute("src")
            book_image_url = urljoin(self.page.url, image_src)
            if description_count > 0:
                book_description = await self.page.inner_text(".product_page p:not([class])")
            else:
                book_description = ''
        except Exception:
            error = Error(self.current_task.url, TaskType.ScrapeBook, "Error while scraping book(title, category, description)", self.scraper_id)
            self.error_queue.put(error)
            await asyncio.sleep(1)
            await self.close()
            return
        
        try:
            is_error = randint(0, 100) < ERROR_RATE
            if is_error:
                raise TimeoutError("TimeoutError")
            table_rows = self.page.locator(".table.table-striped tr")
            data = {}
            rows_count = await table_rows.count()
            for i in range(rows_count):
                row = table_rows.nth(i)
                key = await row.locator("th").text_content()
                value = await row.locator("td").text_content()
                data[key] = value

            book_upc = data["UPC"]
            book_price = data["Price (excl. tax)"]
            book_tax = data["Price (incl. tax)"]
            book_availability = 'in stock' in data["Availability"].lower()
            book_number_of_reviews = data["Number of reviews"]
            bac_text = re.search(r'\d+', data["Availability"]).group()
            if bac_text.isdigit():
                book_availability_count = int(bac_text)
            else:
                book_availability_count = 0
        except Exception:
            error = Error(self.current_task.url, TaskType.ScrapeBook, "Error while scraping book(table)", self.scraper_id)
            self.error_queue.put(error)
            await asyncio.sleep(1)
            await self.close()
            return

        book = Book(
            book_title,
            book_upc,
            book_category,
            book_description,
            book_price,
            book_tax,
            book_availability,
            book_availability_count,
            book_number_of_reviews,
            book_image_url
            )
        self.result_queue.put(book)
        self.current_task = None

