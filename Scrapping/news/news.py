import requests
import pandas as pd
import time
import os 
from dotenv import load_dotenv
dotenv_path = r"C:\Users\medya\OneDrive\Documents\Strong_gradient\Scrapping\.env"
load_dotenv(dotenv_path=dotenv_path)
API_TOKEN = os.getenv("NEWS_API_TOKEN")  
BASE_URL = "https://api.thenewsapi.com/v1/news/all"  

# List of Web3 companies to search for
companies = ["Ethereum", "Solana", "Polygon", "Chainlink", "Uniswap", "Avalanche"]

# Function to fetch news for a given company
def fetch_news(company_name):
    params = {
        "api_token": API_TOKEN,
        "language": "en",  # Fetch English news
        "search": company_name,  # Search for company name in news
        "limit": 3,  # Maximum allowed articles per request (due to free plan limitations)
        "sort": "published_at",  # Sort results by published date
    }

    try:
        print(f"Fetching news for: {company_name}...")
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()  # Raise error for bad status codes
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching news for {company_name}: {e}")
        return None

# Function to process the API response
def process_news(data, company_name):
    if not data or "data" not in data:
        print(f"No news found for {company_name}.")
        return []
    
    news_list = []
    for article in data["data"]:
        news_list.append({
            "Company": company_name,
            "Title": article.get("title"),
            "Description": article.get("description"),
            "Snippet": article.get("snippet"),
            "URL": article.get("url"),
            "Image URL": article.get("image_url"),
            "Published At": article.get("published_at"),
            "Source": article.get("source"),
            "Categories": ", ".join(article.get("categories", [])),
        })
    
    return news_list

# Main execution
if __name__ == "__main__":
    all_news = []
    request_count = 0  # Track API requests
    MAX_REQUESTS = 100  # Free plan allows 100 requests per day

    for company in companies:
        if request_count >= MAX_REQUESTS:  # Stop if daily limit reached
            print("Daily API limit reached. Stopping.")
            break

        news_data = fetch_news(company)
        request_count += 1  # Increment request count
        
        if news_data:
            news_articles = process_news(news_data, company)
            all_news.extend(news_articles)
        
        # API rate-limiting (short delay between requests)
        time.sleep(2)  

    if all_news:
        news_df = pd.DataFrame(all_news)
        news_df.to_csv("web3_companies_news.csv", index=False)
        print("News data saved to 'web3_companies_news.csv' successfully!")

        # Display DataFrame
        #import ace_tools as tools
        #tools.display_dataframe_to_user(name="Web3 Companies News", dataframe=news_df)
    else:
        print("No news articles found.")
