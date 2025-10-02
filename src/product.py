
from dataclasses import dataclass
from typing import Optional


@dataclass
class Product:
    name: str
    description: str
    category: str
    min_price: Optional[int]
    max_price: Optional[int] 
    median_price: Optional[int]
    def __str__(self):
        return f"Product(name={self.name}, description={self.description}, min_price={self.min_price}, max_price={self.max_price}, median_price={self.median_price})"

    def as_tuple(self):
        return (
            self.name,
            self.description,
            self.category,
            self.min_price,
            self.max_price,
            self.median_price,
        )