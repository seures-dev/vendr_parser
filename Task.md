Architectural and Technical Requirements

1. Object-Oriented Programming (OOP):
    1) The entire implementation must follow OOP principles, using inheritance to extend functionality without modifying the core logic.
2. Adherence to Best Programming Practices:
    1) Apply the SOLID principles to ensure flexible and maintainable code.
    2) Follow the DRY (Don't Repeat Yourself) principle to avoid code duplication.
    3) Apply the KISS (Keep It Simple, Stupid) principle to maintain simplicity and clarity.
    4) The code should be readable, well-structured, and thoroughly documented, with meaningful class, method, and variable names.
3. Environment-Based Configuration:
    1) All configuration values (e.g., keys, URLs, modes) must be managed through environment variables to ensure security and configurability.
4. Modularity and Testability:
    1) Each module must be isolated, reusable, and easily testable, with minimal external dependencies.
5. Restrictions:
    1) Usage of SQLAlchemy or similar ORM libraries is strictly prohibited. Database interactions must be implemented via low-level libraries (e.g., psycopg2 or equivalents).

#Task1

Collect all products from the website https://www.vendr.com/ :

● Collect products from only three categories: DevOps, IT Infrastructure, Data Analytics
and Management
● The following data should be collected: product name, category, price range, and
description (refer to the attached photo).
● The scraping must be implemented with requests for fetching data and lxml for parsing.
● Implement a task queue specifically for managing the product scraping tasks. Each
thread should take a task (i.e., a product to scrape) from the queue and process it.
● Use multithreading in your implementation, with multiple threads fetching product data
from the queue and one dedicated thread writing the collected data to the database in
parallel.
● Ensure that all scraping results are stored in the database
