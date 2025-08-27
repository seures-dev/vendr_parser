Implement a web scraping solution for books from the website https://books.toscrape.com/:

● The following data should be collected: title, category, price, rating, stock availability,
image URL, description and all Product Information (refer to the attached photo).

● The scraping must be implemented using Playwright!

● Do not use any other modules for making requests (e.g., requests, aiohttp, etc.) or for
parsing HTML (e.g., BeautifulSoup, lxml, etc.). All operations must be handled
exclusively within Playwright.

● Implement a multiprocessing approach, allowing the number of processes to be
specified when initializing the scraper class. By default, use 3 processes. Each process
must initialize its own browser.

● Create a Process Manager, responsible for ensuring that subprocesses are running
correctly. If any subprocess encounters an issue, the Process Manager should stop the
faulty subprocess and start a new one that continues the work of the previous process.

● Optionally: add support for connecting to the browser via a CDP (Chrome DevTools
Protocol) session.
