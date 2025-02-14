#!/usr/bin/env python3
import os
import time
from datetime import datetime

from pymongo import MongoClient
import pandas as pd

# Selenium imports
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# -------------------------------------------------------------------
# MongoDB Configuration
# -------------------------------------------------------------------
MONGO_URI = "mongodb://localhost:27017/"   
DB_NAME = "finance_db"                     
COLLECTION_NAME = "company_urls"           

# -------------------------------------------------------------------
# Scraping Configuration
# -------------------------------------------------------------------
CSV_OUTPUT = "yahoo_scraped_data.csv"

# Optional: If you want to run Chrome in headless mode (no GUI),
# uncomment the lines under "CHROME OPTIONS" below.
# -------------------------------------------------------------------

def init_driver():
    """
    Initialize a Selenium Chrome WebDriver.
    By default, opens a visible browser. Uncomment
    headless options if you want a silent/hidden browser.
    """
    chrome_options = Options()
    
    # -- HEADLESS MODE 
    # chrome_options.add_argument("--headless")
    # chrome_options.add_argument("--disable-gpu")
    
    # You can add more options as needed:
    # chrome_options.add_argument("--no-sandbox")
    # chrome_options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(
        ChromeDriverManager().install(),
        options=chrome_options
    )
    return driver

def scrape_yahoo_page(driver, url):
    """
    Navigates to the given Yahoo Finance URL using Selenium,
    then scrapes whatever data you need from the page.
    
    Return a dictionary (or any structure) with the extracted info.
    """
    data_dict = {
        "url": url,
        "timestamp": datetime.utcnow().isoformat(),  # e.g.
        "title": None,
        "ticker": None,
        "current_price": None,
        "price_change": None,
        "price_change_percent": None,
        "full_section_text": None
    }
    
    try:
        driver.get(url)
        time.sleep(2)  
        try:
            h1_elem = driver.find_element(By.CSS_SELECTOR, "h1.yf-xxbei9")
            data_dict["title"] = h1_elem.text
        except:
            pass

        if data_dict["title"] and "(" in data_dict["title"]:

            possible_ticker = data_dict["title"].split("(")[-1].replace(")", "")
            data_dict["ticker"] = possible_ticker.strip()

        try:
            price_elem = driver.find_element(By.CSS_SELECTOR, "span[data-testid='qsp-price']")
            data_dict["current_price"] = price_elem.text
        except:
            pass

        # (4) Price change (e.g. "-1.82")
        try:
            change_elem = driver.find_element(By.CSS_SELECTOR, "span[data-testid='qsp-price-change']")
            data_dict["price_change"] = change_elem.text
        except:
            pass

        # (5) Percent change (e.g. "(-0.51%)")
        try:
            pct_elem = driver.find_element(By.CSS_SELECTOR, "span[data-testid='qsp-price-change-percent']")
            data_dict["price_change_percent"] = pct_elem.text
        except:
            pass

        # (6) Full text from the main <section> 
        try:
            main_section = driver.find_element(By.CSS_SELECTOR, "section.main.yf-cfn520")
            data_dict["full_section_text"] = main_section.text
        except:
            pass

    except Exception as e:
        print(f"❌ [Error] {url}: {e}")

    return data_dict

def main():
    # 1) Connect to MongoDB and fetch docs
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    # Example query: get all docs with "active": True
    docs_cursor = collection.find({"active": True})
    docs_list = list(docs_cursor)

    # 2) Filter out only the URLs that contain "yahoo"
    yahoo_docs = [doc for doc in docs_list if "url" in doc and "yahoo" in doc["url"].lower()]

    # 3) Initialize Selenium WebDriver (Chrome)
    driver = init_driver()

    # 4) Scrape each Yahoo URL
    all_results = []
    for doc in yahoo_docs:
        url = doc["url"]
        print(f"Visiting: {url} ...")
        result = scrape_yahoo_page(driver, url)
        all_results.append(result)

    # 5) Close the driver
    driver.quit()

    # 6) Convert results to DataFrame and export CSV
    df = pd.DataFrame(all_results)
    df.to_csv(CSV_OUTPUT, index=False)
    print(f"\n✅ Done! Results saved to '{CSV_OUTPUT}'")

    # Close Mongo connection
    client.close()

if __name__ == "__main__":
    main()
