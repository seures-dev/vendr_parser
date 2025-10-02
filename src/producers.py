import re
import threading
from queue import Queue
from typing import Optional, List
from urllib.parse import urlparse, urlunparse
from lxml import html, etree
from src.http_client import HttpClient
from src.logger import get_logger


class CategoryProducer:
    """
    Collects product URLs from category pages and enqueues them into the task queue.
    Designed to be simple: iterate listing pages until no new links found or a safe max page cap.
    """

    def __init__(
        self,
        http_client: HttpClient,
        category_urls: List[str],
        task_queue: Queue,
        stop_event: threading.Event = None
    ) -> None:
        self.http_client = http_client
        self.category_urls = category_urls
        self.task_queue = task_queue
        self.stop_event = stop_event or threading.Event()
        self.sub_category_urls = Queue()
        self.logger = get_logger("CategoryProducer")

    def produce(self) -> None:
        """
        Orchestrates the scraping workflow for product links.

        Workflow:
            1. Iterates over the configured category URLs.
            2. For each category, scrapes subcategory links.
            3. If a stop event is set, the process stops early.
            4. Collects subcategory URLs from the queue and scrapes
            their listing pages to extract product links.
            5. Logs the start and finish of the producer workflow.

        This method does not return anything but enqueues product-related tasks
        discovered during the scraping process.
        """
        self.logger.info("Producer starting for categories: %s", self.category_urls)
        for category_url in self.category_urls:

            self._scrap_subcategory_links(category_url)
            if self.stop_event.is_set():
                break
        while not self.sub_category_urls.empty():
            link, category_hint = self.sub_category_urls.get()
            self._scrape_listing_pages(link, category_hint)
        self.logger.info("Producer finished enqueuing tasks.")

    def _scrap_subcategory_links(self, category_url: str) -> None:
        html_text = self.http_client.fetch(category_url)
        if not html_text:
            return
        doc = html.fromstring(html_text)
        parsed_url = urlparse(category_url)
        main_url = urlunparse((parsed_url.scheme, parsed_url.netloc, '', '', '', ''))
        category_hint = None
        try:
            category_hint = self._get_category_text(doc)
        except etree.XPathError:
            pass

        if category_hint is None:
            category_hint = parsed_url.path.split('/')[-1].capitalize()

        try:
            links_elements = doc.xpath('//div[contains(@class, "rt-BaseCard")]'
                                       '//span[contains(text(), "View more")]/..')
            if not links_elements:
                return
            for element in links_elements:
                # creating task for scraping subcategory
                self.sub_category_urls.put(
                    (main_url + element.get('href'), category_hint)
                )
        except Exception as ex:

            print("Error extracting subcategories links: %s", ex)
            return

    def _get_category_text(self, doc) -> Optional[str]:
        try:
            elements = doc.xpath('//h1[contains(@class, "rt-Heading")]')
            for el in elements:
                element_text = el.text
                if not element_text:
                    continue
                return element_text.strip()
        except etree.XPathEvalError as ex:
            self.logger.exception("Invalid XPath expression: %s", ex)
        except AttributeError as ex:
            self.logger.exception("Invalid doc object passed, no xpath method: %s", ex)
        except Exception as ex:
            self.logger.exception("Unexpected error while extracting category text: %s", ex)
        return None

    def _scrape_listing_pages(self, first_url: str, category_hint: str):
        """Scraping listing pages"""       
        def scrap_paggination(doc):
            pagination_element = doc.xpath(
                '//div[contains(@class, "rt-r-ai-center")]//'
                'span[contains(string(), "Page") and contains(string(), "of")]'
            )[0]
            pagination_text = pagination_element.xpath('string()').strip()
            current_page_text, max_page_text = re.findall(r'\d+', pagination_text)
            _page = int(current_page_text)
            _max_pages = int(max_page_text)
            return _page, _max_pages
            
        page = 1
        page_count = 0  # safety cap
        page_count_set = False
        url = first_url
        sub_category = ''
        parsed_url = urlparse(first_url)
        main_url = urlunparse((parsed_url.scheme, parsed_url.netloc, '', '', '', ''))
        # scraping subcategory pages one by o
        while (page <= page_count or not page_count_set) and not self.stop_event.is_set():
            html_text = self.http_client.fetch(url)
            print(url)
            if not html_text:
                continue
            try:
                doc = html.fromstring(html_text)
                product_cards = doc.xpath('//a[contains(@class,'
                                          ' "_card_1u7u9_1 _cardLink_1q928_1")]')
            except Exception as e:

                print(f"Error parsing product cards page: {e}")
                return
            if not page_count_set:
                sub_category = self._get_category_text(doc)
                # scraping current page and page count
                page, page_count = scrap_paggination(doc)
                page_count_set = True      
            if not product_cards:
                return None

            try:
                full_category = f"{category_hint} - {sub_category}"
                for product_card in product_cards:
                    # scrating task for scraping product
                    self.task_queue.put(
                        (main_url + product_card.get('href'), full_category)
                    )
            except Exception as ex:
                self.logger.exception("Unexpected error while create task: %s", ex)

            url = self._increment_page_param(url)
            page += 1

    def _increment_page_param(self, url: str) -> str:
        """Increment page"""
        m = re.search(r"page=(\d+)", url)
        if m:
            cur = int(m.group(1))
            return url.replace(f"page={cur}", f"page={cur+1}")
        else:
            sep = "&" if "?" in url else "?"
            return url + f"{sep}page=2"
