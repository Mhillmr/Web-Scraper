import os
import logging
import time
from urllib.parse import urljoin, unquote

import requests
from bs4 import BeautifulSoup
import psycopg2
from psycopg2 import OperationalError, Error
from dotenv import load_dotenv


# Load environment variables
load_dotenv()

# Constants
BASE_URL = "https://emalls.ir/"
PAGES = range(1, 11)  # Number of pages to crawl
REQUEST_DELAY = 1  # seconds

# Database configuration
DB_CONFIG = {
    "dbname": os.environ["EMALLS_DB"],
    "user": os.environ["EMALLS_USER"],
    "password": os.environ["EMALLS_PW"],
    "host": os.environ["EMALLS_HOST"],
    "port": os.environ["EMALLS_PORT"],
}


def configure_root_logger() -> logging.Logger:
    """
    Configure the root logger for console output.
    """
    logger = logging.getLogger(__name__)
    if not logger.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s: %(message)s",
        )
    return logger


logger = configure_root_logger()

# Configure failed-shops logger (writes to failed_shops.log)
failed_logger = logging.getLogger("failed_shops")
if not failed_logger.handlers:
    failed_logger.setLevel(logging.INFO)
    fh = logging.FileHandler("failed_shops.log", encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
    failed_logger.addHandler(fh)


 # Database utility functions
 
def table_exists(conn: psycopg2.extensions.connection, name: str) -> bool:
    """
    Check if a table exists in the public schema.
    Returns True if exists, False otherwise.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS (
                  SELECT 1 FROM information_schema.tables
                  WHERE table_schema='public' AND table_name=%s
                );
                """,
                (name,),
            )
            return cur.fetchone()[0]
    except Error as e:
        logger.error(f"[table_exists] Error checking table '{name}': {e}")
        return False


def init_db(conn: psycopg2.extensions.connection) -> None:
    """
    Initialize the database by creating the emalls_shops table if missing.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS emalls_shops (
                  id          SERIAL PRIMARY KEY,
                  source      TEXT    NOT NULL,
                  is_assigned BOOLEAN NOT NULL,
                  domain      TEXT UNIQUE,
                  emalls_id   TEXT,
                  address     TEXT,
                  tel         TEXT[],
                  owner       TEXT,
                  category    TEXT[]
                );
                """
            )
        conn.commit()
        logger.info("Database schema ensured")
    except Error as e:
        logger.exception(f"[init_db] Error initializing schema: {e}")
        conn.rollback()


def insert_shop(conn: psycopg2.extensions.connection, info: dict) -> int | None:
    """
    Insert or update a shop record, returning the record ID.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO emalls_shops
                  (source, is_assigned, domain, emalls_id, address, tel, owner, category)
                VALUES (%(source)s, %(is_assigned)s, %(domain)s, %(emalls_id)s,
                        %(address)s, %(tel)s, %(owner)s, %(category)s)
                ON CONFLICT (domain) DO UPDATE
                  SET source      = EXCLUDED.source,
                      is_assigned = EXCLUDED.is_assigned,
                      emalls_id   = EXCLUDED.emalls_id,
                      address     = EXCLUDED.address,
                      tel         = EXCLUDED.tel,
                      owner       = EXCLUDED.owner,
                      category    = EXCLUDED.category
                RETURNING id;
                """,
                info,
            )
            row = cur.fetchone()
        conn.commit()
        return row[0] if row else None
    except Error as e:
        logger.exception(f"[insert_shop] DB error for '{info.get('domain')}': {e}")
        conn.rollback()
        return None


 # Scraper functions
 
def extract_shop_info(soup: BeautifulSoup, shop_url: str) -> dict:
    """
    Extract key info from a shop page soup.
    Returns dict with defaults: empty strings or lists.
    """
    info = {
        "source": "emalls",
        "is_assigned": False,
        "domain": "",
        "emalls_id": shop_url,
        "address": "",
        "tel": [],
        "owner": "",
        "category": [],
    }

    # 1) Website URL
    tag = soup.find(id="ContentPlaceHolder1_HlkWebsite2")
    if tag and tag.has_attr("href"):
        info["domain"] = tag["href"].strip()

    # 2) Address
    tag = soup.find(id="ContentPlaceHolder1_lblAddress2")
    if tag:
        info["address"] = tag.get_text(strip=True)

    # 3) Telephone(s)
    tag = soup.find(id="ContentPlaceHolder1_HlkTelephone2")
    if tag and tag.has_attr("href"):
        raw = tag["href"].split(":", 1)[-1]
        decoded = unquote(raw)
        parts = [p.strip() for p in decoded.split("،") if p.strip()]
        info["tel"] = parts

    # 4) Owner
    tag = soup.find(id="ContentPlaceHolder1_lblMasool2")
    if tag:
        info["owner"] = tag.get_text(strip=True)

    # 5) Categories (max 5)
    container = soup.find(id="DivPartProducts")
    if container:
        titles = container.select(".shopcat-box .title")[:5]
        cats = [t.get_text(strip=True) for t in titles if t.get_text(strip=True)]
        info["category"] = cats or []

    return info


 # Main
 
def main() -> None:
    logger.info("Starting scraper")

    # Connect to database
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            # Ensure table exists
            if not table_exists(conn, "emalls_shops"):
                init_db(conn)

            # Fetch existing shop IDs
            with conn.cursor() as cur:
                cur.execute("SELECT emalls_id FROM emalls_shops;")
                urls_in_db = {r[0] for r in cur.fetchall()}

            shop_urls = []

            # Crawl list pages
            with requests.Session() as session:
                session.headers.update({"User-Agent": "Mozilla/5.0"})
                for i in PAGES:
                    page_url = f"{BASE_URL}Shops/page.{i}/"
                    logger.info(f"Fetching list page: {page_url}")
                    try:
                        r = session.get(page_url, timeout=10)
                    except requests.exceptions.RequestException as e:
                        logger.error(f"List page request failed: {e}")
                        failed_logger.info(page_url)
                        continue

                    if not r.ok:
                        logger.error(f"List page returned {r.status_code}")
                        failed_logger.info(page_url)
                        continue

                    soup = BeautifulSoup(r.text, "html.parser")
                    for a in soup.select("div.shop a.shop-ax"):
                        href = a.get("href")
                        if href:
                            shop_urls.append(urljoin(BASE_URL, href))

                    time.sleep(REQUEST_DELAY)

                # Process each shop
                for shop_url in shop_urls:
                    if shop_url in urls_in_db:
                        logger.info(f"Skipping already-seen shop: {shop_url}")
                        continue

                    logger.info(f"Processing shop: {shop_url}")
                    try:
                        r = session.get(shop_url, timeout=10)
                    except requests.exceptions.RequestException as e:
                        logger.error(f"Shop page request failed: {e}")
                        failed_logger.info(shop_url)
                        continue

                    if not r.ok:
                        logger.error(f"Shop page returned {r.status_code}")
                        failed_logger.info(shop_url)
                        continue

                    soup = BeautifulSoup(r.text, "html.parser")
                    info = extract_shop_info(soup, shop_url)
                    shop_id = insert_shop(conn, info)
                    if not shop_id:
                        failed_logger.info(shop_url)
                        logger.warning(f"Failed to save shop: {shop_url}")
                    else:
                        logger.info(f"Saved shop id={shop_id}")

                    time.sleep(REQUEST_DELAY)

    except OperationalError as e:
        logger.error(f"DB connection failed: {e}")

    logger.info("Scraping complete")


if __name__ == "__main__":
    main()
