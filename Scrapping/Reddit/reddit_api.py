import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import praw
import pandas as pd
from dotenv import load_dotenv

# ---------------------------
# Load environment variables
# ---------------------------
load_dotenv()
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT")

# ---------------------------
# Reddit API Credentials
# ---------------------------
REDDIT = praw.Reddit(
    client_id=REDDIT_CLIENT_ID,
    client_secret=REDDIT_CLIENT_SECRET,
    user_agent=REDDIT_USER_AGENT
)

# ---------------------------
# Configuration
# ---------------------------
SUBREDDITS = [
    "ethdev", "solana", "AlgorandOfficial", "web3", "CryptoTechnology", "ethstaker",
    "evmos", "EthereumClassic", "Polkadot", "BinanceSmartChain", "defi", "CryptoMarkets",
    "CryptoCurrencies", "Bitcoin", "ethfinance", "NFT", "0xPolygon", "Chainlink",
    "CosmosNetwork", "Stellar"
]

QUERIES = [
    "web3", "blockchain", "crypto", "decentralized finance", "smart contract", "NFT",
    "Ethereum", "Solana", "Rust", "EVM", "DeFi", "staking", "airdrops", "wallet",
    "AI", "machine learning", "deep learning", "LLM", "sustainability", "green blockchain",
    "web scraping", "Ethereum API", "Binance API", "Solana RPC", "zkSNARK", "Layer 2",
    "flash loan attack", "rug pull detection"
]

MAX_POSTS = 100  
MAX_WORKERS = 10  
CSV_FILENAME = "reddit_scraped_data.csv"

# ---------------------------
# Scrape Function
# ---------------------------
def scrape_subreddit(subreddit: str, query: str) -> list:
    """Scrapes a single subreddit with a given query and returns post data as a list of lists."""
    results = []
    try:
        for submission in REDDIT.subreddit(subreddit).search(query, limit=MAX_POSTS):
            results.append([
                subreddit,
                query,
                submission.title,
                submission.selftext,
                submission.author.name if submission.author else "Unknown",
                submission.score,
                submission.num_comments,
                submission.url,
                time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(submission.created_utc))
            ])
    except Exception as e:
        print(f"Error scraping r/{subreddit} for '{query}': {e}")
    return results

# ---------------------------
# Main Execution
# ---------------------------
def main():
    start_time = time.time()
    all_data = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        # Submit all jobs
        for subreddit in SUBREDDITS:
            for query in QUERIES:
                futures.append(executor.submit(scrape_subreddit, subreddit, query))
        
        # Gather all results as they complete
        for future in as_completed(futures):
            all_data.extend(future.result())

    # Create a DataFrame and export to CSV
    df = pd.DataFrame(all_data, columns=[
        "Subreddit", "Query", "Title", "Text", "Author",
        "Upvotes", "Comments", "URL", "Timestamp"
    ])
    df.to_csv(CSV_FILENAME, index=False)

    end_time = time.time()
    print(f"Scraping complete! Data saved to '{CSV_FILENAME}'")
    print(f"Total Execution Time: {round(end_time - start_time, 2)} seconds")

if __name__ == "__main__":
    main()
