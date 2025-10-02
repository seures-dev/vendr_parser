import re
from typing import Iterable, Optional, Tuple
from lxml import html
from src.logger import get_logger
from src.product import Product


# from .http_client import HttpClient

class ProductParser:
    """Parses HTML strings into Product dataclasses using lxml and heuristics."""
    def __init__(self) -> None:
        self.logger = get_logger("ProductParser")

    def parse_product_page(
        self,
        page_html: str,
        product_url: str,
        category_hint: str
    ) -> Optional[Product]:
        """Parse product fields from product detail HTML."""
        try:
            doc = html.fromstring(page_html)
        except Exception as e: 
            self.logger.debug("Failed to parse HTML for %s: %s", product_url, e)
            raise

        product_name_element = doc.xpath('//h1[contains(@class, "rt-Heading")]')
        name = self._first_text(product_name_element)

        description = (
            self._first_text(doc.xpath('//div[contains(@class, "_read-more-box'
                                       '__content_122o3_1")]//p[contains(@class, "rt-Text")]'))
            or self._first_text(product_name_element[0].xpath('.//../..//p'))
        )

        # finding price range
        median_price, min_price, max_price = self._find_price_text(doc)

        # Trim
        name = name.strip()
        description = description.strip()
        if not name:
            # If no name found, skip
            self.logger.debug("No name parsed for %s; skipping", product_url)
            return None

        return Product(
            name=name,
            category=category_hint.strip(),
            median_price=median_price,
            min_price=min_price,
            max_price=max_price,
            description=description,
        )

    def _first_text(self, elements: Iterable) -> Optional[str]:
        for el in elements:
            if el is None:
                continue
            text = ""
            if isinstance(el, str):
                text = el.strip()
            else:
                text = el.text_content().strip()
            if text:
                return text
        return None

    def _find_price_text(self, doc) -> Tuple[Optional[int], Optional[int], Optional[int]]:
        # search elements that may contain pricing-related keywords
        try:
            mediana_elements = doc.xpath('//div[contains(@class, "_rangeAverage_118fo_42")]'
                                         '/text()[normalize-space()]')
            if mediana_elements:
                median_price_text = mediana_elements[0]
                min_price_text, max_price_text = doc.xpath(
                    '//div[contains(@class, "_rangeSlider_118fo_13")]//span/text()'
                )
                median_price = self._parse_number(median_price_text)
                min_price = self._parse_number(min_price_text)
                max_price = self._parse_number(max_price_text)
            else:
                median_price = None
                min_price = None
                max_price = None
            return (median_price, min_price, max_price)
        except (ValueError, IndexError) as e:
            print(f"Error extracting product details: {e}")
            return (None, None, None)

    def _parse_number(self, text: str) -> Optional[int]:
        """Return number(int) from text"""
        try:
            number = int(re.findall(r'[\d,]+(?:\.\d+)?', text.replace(',', '').strip())[0])
            return number
        except (ValueError, TypeError):
            return None
        except Exception as ex:
            self.logger.exception(
                "Unexpected error while parsing number from text %s, Error: %s", text, ex
            )
            raise
