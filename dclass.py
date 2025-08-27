from dataclasses import dataclass
# from enum import Enum

class TaskType:
    ScrapeBookLinks = "scrape_book_links"
    ScrapeBook = "scrape_book"


@dataclass
class Task:
    url: str
    task_type: TaskType

@dataclass
class Error(Task):
    error: str
    scraper_id: int

@dataclass
class Book:
    book_title: str
    book_upc: str
    book_category: str
    book_description: str
    book_price: str
    book_tax: str
    book_availability: bool
    book_availability_count: int
    book_number_of_reviews: int

