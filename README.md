# Emalls Scraper

A web scraper that extracts shop information from [emalls.ir](https://emalls.ir/) and stores it in a PostgreSQL database.

## Features
- Scrapes shop pages using `requests` + `BeautifulSoup`
- Saves data into PostgreSQL with `ON CONFLICT` handling
- Logs failed shop pages into `failed_shops.log`
- Uses environment variables (`.env`) for configuration
- Simple, modular, and easy to extend

## Requirements
- Python 3.10+
- PostgreSQL database

## Installation

Clone the repository and set up a virtual environment:

```bash
git clone https://github.com/your-username/emalls-scraper.git
cd emalls-scraper
python -m venv venv
source venv/bin/activate   # On Windows: venv\Scripts\activate
pip install -r requirements.txt
