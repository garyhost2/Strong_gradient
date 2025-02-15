from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import re
import time
import requests
import json

# Configure Chrome options
options = Options()
options.headless = False  
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                     "AppleWebKit/537.36 (KHTML, like Gecko) "
                     "Chrome/113.0.0.0 Safari/537.36")

driver = webdriver.Chrome(options=options)


def extract_pagination_integer(driver, timeout=20):
    selector = (
        "#root > div.App.lang-en > div.sc-dmjyfX.lavnNz.Container.sc-fNYidB.lmTbCu > section > "
        "div.sc-iySxkz.hfIPwu > div.sc-dmBZcA.sc-dxBvky.bvcmCI.enyxzJ > "
        "div.sc-gikAfH.gfPAvV.pagination-selector > div > a:last-child"
    )
    element = WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
    )
    text = element.text
    print(f"Extracted pagination text: '{text}'")
    match = re.search(r'\d+', text)
    return int(match.group()) if match else None


def push_data_to_api(companies):
    """Push collected company data to the API."""
    if not companies:
        return  # Avoid pushing empty data
    
    url = "http://localhost:8080/api/v1/companies/batch"
    payload = json.dumps({"companies": companies})
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url, data=payload, headers=headers)
    print(f"API Response: {response.status_code}, {response.text}")


try:
    base_url = "https://dappradar.com/web3-ecosystem"
    driver.get(base_url)
    time.sleep(5)
    
    page_number = extract_pagination_integer(driver)
    print(f"Pagination integer: {page_number}")
    
    companies = []
    batch_size = 25
    
    for i in range(1, page_number + 1):
        page_url = f"{base_url}/{i}"
        print(f"\nProcessing page: {page_url}")
        driver.get(page_url)
        WebDriverWait(driver, 30).until(lambda d: d.execute_script("return document.readyState") == "complete")
        time.sleep(5)
        
        cards = WebDriverWait(driver, 30).until(
            EC.presence_of_all_elements_located(
                (By.CSS_SELECTOR,
                 "#root > div.App.lang-en > div.sc-dmjyfX.lavnNz.Container.sc-fNYidB.lmTbCu > section > "
                 "div.sc-iySxkz.hfIPwu > div.sc-gNghfG.dvYAwN > div > a")
            )
        )
        print(f"Found {len(cards)} cards on page {i}.")
        
        for idx, card in enumerate(cards, start=1):
            try:
                url = card.get_attribute("href")
                if url:
                    company_name = url.split('/')[-1].replace("-", " ")
                    companies.append({"name": company_name, "status": "not activated"})
                    
                    # Push data every 25 companies
                    if len(companies) >= batch_size:
                        push_data_to_api(companies)
                        companies = []  # Clear list after pushing
                    
                else:
                    print(f"Card {idx} does not have a valid href attribute.")
            except Exception as e:
                print(f"Error extracting company name for card {idx}: {e}")

    # Final push if any remaining companies exist
    if companies:
        push_data_to_api(companies)

finally:
    driver.quit()
