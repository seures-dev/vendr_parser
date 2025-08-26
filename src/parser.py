import re
import time
import requests
from queue import Empty, Queue
from urllib.parse import urlparse, urlunparse
from lxml import html
from typing import Optional

class TaskType:
    EXTRACT_SUBCATEGORIES = "extract_subcategories"
    EXTRACT_PRODUCT_LINKS = "extract_product_links"
    EXTRACT_PRODUCT_DETAILS = "extract_product_details"


class Task:
    def __init__(self, task_type: TaskType, url: str, referer: str):
        self.task_type: TaskType = task_type
        self.url: str = url
        self.referer: str = referer
    def __str__(self):
        return f"Task(task_type={self.task_type}, url={self.url}, referer={self.referer})"


class ProductDetails:
    def __init__(self, name: str, description: str, min_price: Optional[int], max_price: Optional[int], median_price: Optional[int]):
        self.name: str = name
        self.description: str = description
        self.min_price: Optional[int] = min_price
        self.max_price: Optional[int] = max_price
        self.median_price: Optional[int] = median_price
    def __str__(self):
        return f"ProductDetails(name={self.name}, description={self.description}, min_price={self.min_price}, max_price={self.max_price}, median_price={self.median_price})"


class Parser:

    def __init__(self, task_queue: Queue, result_queue: Queue, user_agent: str):
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.user_agent = user_agent
        self.headers = {
            "User-Agent": self.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "DNT": "1",
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        
    def _save_debug_html(self, html_text: str, filename: str):
        with open(f"debug/{filename}.html", "w", encoding="utf-8") as f:
            f.write(html_text)
        
    def run(self):
        while True:
            try:
                task = self.task_queue.get(timeout=10)
            except Empty:
                print("Parser task queue is empty")
                break

            if task.task_type == TaskType.EXTRACT_SUBCATEGORIES:
                self.extract_subcategories_links(task.url, task.referer)
            elif task.task_type == TaskType.EXTRACT_PRODUCT_LINKS:
                self.exctract_products_links(task.url, task.referer)
            elif task.task_type == TaskType.EXTRACT_PRODUCT_DETAILS:
                self.extract_product_details(task.url, task.referer)
        print("Parser stopped")

    def _request(self, url: str, referer: str):
        try:    
            self.session.headers.update({"Referer": referer})
            response = self.session.get(url)
            response.raise_for_status()
            return response.text
        except Exception as e:
            print(f"Error requesting {url}: {e}")
            return None
    
    def extract_subcategories_links(self, sub_category_url: str, referer: str):
        
        html_text = self._request(sub_category_url, referer)
        if not html_text:
            return
        
        try:
            tree = html.fromstring(html_text)
        except Exception as e:
            self._save_debug_html(html_text, f"category_page_parse_error_{time.time()}")
            print(f"Error parsing category page: {e}")
            return
        try:
            parsed_url = urlparse(sub_category_url)
            main_url = urlunparse((parsed_url.scheme, parsed_url.netloc, '', '', '', ''))
            links_elements = tree.xpath('//div[contains(@class, "rt-BaseCard")]//span[contains(text(), "View more")]/..')
            if not links_elements:
                return
            for element in links_elements:
                self.task_queue.put(Task(TaskType.EXTRACT_PRODUCT_LINKS, main_url + element.get('href'), sub_category_url))
        except Exception as e:
            self._save_debug_html(html_text, f"subcategories_links_parse_error_{time.time()}")
            print(f"Error extracting subcategories links: {e}")
            return
        self.task_queue.task_done()

    def exctract_products_links(self, sub_category_url: str, referer: str):
 
        html_text = self._request(sub_category_url, referer)
        if not html_text:
            return
        
        try:
            tree = html.fromstring(html_text)
            product_cards = tree.xpath('//a[contains(@class, "_card_1u7u9_1 _cardLink_1q928_1")]')
        except Exception as e:
            self._save_debug_html(html_text, f"product_cards_parse_error_{time.time()}")
            print(f"Error parsing product cards page: {e}")
            return
        
        parsed_url = urlparse(sub_category_url)
        main_url = urlunparse((parsed_url.scheme, parsed_url.netloc, '', '', '', ''))
        
        if not product_cards:
            return 
        
        try:
            for product_card in product_cards:
                task_url = main_url + product_card.get('href')
                self.task_queue.put(Task(TaskType.EXTRACT_PRODUCT_DETAILS, task_url, sub_category_url))
        except Exception as e:
            self._save_debug_html(html_text, f"products_links_parse_error_{time.time()}")
            print(f"Error extracting products links: {e}")
            return
        try:
            pagination_element = tree.xpath('//div[contains(@class, "rt-r-ai-center")]//span[contains(string(), "Page") and contains(string(), "of")]')[0]
            pagination_text = pagination_element.xpath('string()').strip()
            current_page_text, max_page_text = re.findall(r'\d+', pagination_text)
            current_page = int(current_page_text)
            max_page = int(max_page_text)
            if max_page > 1 and current_page == 1:
                base_url = sub_category_url.split('?page=')[0]
                for page in range(2, max_page + 1):
                    link = base_url + f"?page={page}"
                    self.task_queue.put(Task(TaskType.EXTRACT_PRODUCT_LINKS, link, sub_category_url))
        except Exception as e:
            self._save_debug_html(html_text, f"pagination_parse_error_{time.time()}")
            print(f"Error extracting pagination: {e}")
            return
        self.task_queue.task_done()

    def extract_product_details(self, product_url: str, referer: str):
        html_text = self._request(product_url, referer)
        if not html_text:
            return
        
        try:
            tree = html.fromstring(html_text)
        except Exception as e:
            print(f"Error parsing product details page: {e}")
            return
        try:
            product_name_element = tree.xpath('//h1[contains(@class, "rt-Heading")]')
            product_name = product_name_element[0].text
        except Exception as e:
            print(f"Error extracting product name: {e}")
            return
        try:
            product_description_element = tree.xpath('//div[contains(@class, "_read-more-box__content_122o3_1")]//p[contains(@class, "rt-Text")]')
            if not product_description_element:
                #if no description block, use short description from
                product_description = product_name_element[0].xpath('//../..//p/text()')[0].strip()
            else:
                product_description = product_description_element[0].text_content().strip()
                test_descriotion = product_name_element[0].xpath('//../..//p/text()')[0].strip()
                if test_descriotion != product_description:
                    print(f"Description mismatch: {test_descriotion} != {product_description}")
        except Exception as e:
            print(f"Error extracting product description: {e}")
            return

        try:
            mediana_elements = tree.xpath('//div[contains(@class, "_rangeAverage_118fo_42")]/text()[normalize-space()]')
            if mediana_elements:
                median_price_text = mediana_elements[0]
                min_price_text, max_price_text = tree.xpath('//div[contains(@class, "_rangeSlider_118fo_13")]//span/text()')
                median_price = int(re.findall(r'[\d,]+(?:\.\d+)?', median_price_text.replace(',', '').strip())[0])
                min_price = int(re.findall(r'[\d,]+(?:\.\d+)?', min_price_text.replace(',', '').strip())[0])
                max_price = int(re.findall(r'[\d,]+(?:\.\d+)?', max_price_text.replace(',', '').strip())[0])
            else:
                median_price = None
                min_price = None
                max_price = None
            product = ProductDetails(product_name, product_description, min_price, max_price, median_price)
            self.result_queue.put(product)
            self.task_queue.task_done()
            # print(product)
        except (ValueError, IndexError) as e:
            print(f"Error extracting product details: {e}")
            return

