import os
import time
import json
import logging
import sys
from typing import Any, Dict, List

# ---------------------------
# MongoDB
# ---------------------------
from pymongo import MongoClient

# ---------------------------
# Reddit
# ---------------------------
import praw
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------------------------
# News API
# ---------------------------
import requests

# ---------------------------
# GitHub (async scraping)
# ---------------------------
import aiohttp
import asyncio
import base64

# ---------------------------
# Neo4j
# ---------------------------
from neo4j import GraphDatabase, basic_auth

# ---------------------------
# Environment / .env
# ---------------------------
from dotenv import load_dotenv

# -------------------------------------------------------------------
# 1) Load Environment Variables & Configure Logging
# -------------------------------------------------------------------
load_dotenv()  # Load from .env if present

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

# MongoDB Connection
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "mydb")
MONGO_COLLECTION = os.getenv("MONGO_COMPANIES_COLLECTION", "companies")

# Neo4j Connection
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

# Reddit Credentials
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT")

# News API
NEWS_API_TOKEN = os.getenv("NEWS_API_TOKEN")  # thenewsapi.com
NEWS_API_BASE_URL = "https://api.thenewsapi.com/v1/news/all"

# GitHub
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
GITHUB_API_URL = 'https://api.github.com/search/repositories'
GITHUB_CONTENTS_API_URL = 'https://api.github.com/repos/{owner}/{repo}/contents/README.md'

# DeFi Llama
DEFILLAMA_BASE_URL = "https://api.llama.fi"

# -------------------------------------------------------------------
# 2) Set up Global Clients
# -------------------------------------------------------------------
# Mongo Client
mongo_client = MongoClient(MONGODB_URI)
mongo_db = mongo_client[MONGO_DB_NAME]
mongo_collection = mongo_db[MONGO_COLLECTION]

# Neo4j Driver
driver = GraphDatabase.driver(NEO4J_URI, auth=basic_auth(NEO4J_USER, NEO4J_PASSWORD))

# Reddit Client
REDDIT = praw.Reddit(
    client_id=REDDIT_CLIENT_ID,
    client_secret=REDDIT_CLIENT_SECRET,
    user_agent=REDDIT_USER_AGENT
)

# -------------------------------------------------------------------
# 3) Helper: Get List of Companies from MongoDB
#    Assuming each document in the Mongo collection has a field "name".
# -------------------------------------------------------------------
def get_companies_from_mongo() -> List[str]:
    """Fetch company names from MongoDB."""
    companies = []
    for doc in mongo_collection.find({}, {"_id": 0, "name": 1}):
        if "name" in doc and doc["name"]:
            companies.append(doc["name"])
    return companies

# -------------------------------------------------------------------
# 4) Scraper #1: Reddit
#    We can keep the existing subreddits and queries,
#    or adapt them to use the companies from Mongo if needed.
# -------------------------------------------------------------------
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

MAX_REDDIT_POSTS = 50  
MAX_WORKERS = 10

def scrape_subreddit(subreddit: str, query: str) -> List[Dict[str, Any]]:
    """Scrapes a single subreddit for a given query."""
    results = []
    try:
        for submission in REDDIT.subreddit(subreddit).search(query, limit=MAX_REDDIT_POSTS):
            results.append({
                "subreddit": subreddit,
                "query": query,
                "title": submission.title,
                "text": submission.selftext,
                "author": submission.author.name if submission.author else "Unknown",
                "upvotes": submission.score,
                "comments": submission.num_comments,
                "url": submission.url,
                "timestamp": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(submission.created_utc))
            })
    except Exception as e:
        logger.warning(f"Error scraping r/{subreddit} for '{query}': {e}")
    return results

def run_reddit_scraper() -> List[Dict[str, Any]]:
    """Run the Reddit scraper concurrently, returning a list of all posts."""
    all_data = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for subreddit in SUBREDDITS:
            for query in QUERIES:
                futures.append(executor.submit(scrape_subreddit, subreddit, query))
        for f in as_completed(futures):
            all_data.extend(f.result())
    logger.info(f"[Reddit] Scraped {len(all_data)} total posts.")
    return all_data

# -------------------------------------------------------------------
# 5) Scraper #2: News API
#    We use the company names from MongoDB for the search.
# -------------------------------------------------------------------
def fetch_news_for_company(company: str) -> List[Dict[str, Any]]:
    """Fetch news articles from thenewsapi.com for a single company."""
    params = {
        "api_token": NEWS_API_TOKEN,
        "language": "en",
        "search": company,
        "limit": 3,
        "sort": "published_at",
    }
    try:
        resp = requests.get(NEWS_API_BASE_URL, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if "data" not in data:
            return []
        articles = []
        for article in data["data"]:
            articles.append({
                "company": company,
                "title": article.get("title"),
                "description": article.get("description"),
                "snippet": article.get("snippet"),
                "url": article.get("url"),
                "image_url": article.get("image_url"),
                "published_at": article.get("published_at"),
                "source": article.get("source"),
                "categories": article.get("categories", [])
            })
        return articles
    except Exception as e:
        logger.warning(f"Error fetching news for '{company}': {e}")
        return []

def run_news_scraper(companies: List[str]) -> List[Dict[str, Any]]:
    """Run the News API scraper for a list of companies."""
    all_news = []
    # Respect free-plan rate limits: 100 requests/day. Sleep if needed.
    for idx, company in enumerate(companies):
        if idx >= 90:  # safety margin
            logger.info("Reached ~90 requests, stopping to avoid daily limit.")
            break
        results = fetch_news_for_company(company)
        all_news.extend(results)
        time.sleep(2)  # short delay
    logger.info(f"[NewsAPI] Fetched {len(all_news)} news articles.")
    return all_news

# -------------------------------------------------------------------
# 6) Scraper #3: GitHub
#    Removed Flask. We'll just define a function that fetches repos
#    relevant to "web3" or optionally to your Mongo companies.
# -------------------------------------------------------------------
WEB3_KEYWORDS = [
    "web3", "ethereum", "blockchain", "cryptocurrency", "defi", "nft",
    "dapp", "dao", "smartcontract", "cosmos", "solana", "decentralized"
]

# Example best-practice docs for scoring
WEB3_BEST_PRACTICE_DOC = """web3 ethereum blockchain decentralization smartcontract defi dapp dao ipfs cosmos cryptography trustless transparency"""
SUSTAINABILITY_BEST_PRACTICE_DOC = """sustainability blockchain proof-of-stake energy efficiency carbon offset renewable energy low-carbon eco-friendly"""

def jaccard_similarity(str1: str, str2: str) -> float:
    set1 = set(str1.lower().split())
    set2 = set(str2.lower().split())
    if not set1 and not set2:
        return 0.0
    return len(set1.intersection(set2)) / len(set1.union(set2))

def calculate_web3_sustainability_scores(readme_text: str) -> Dict[str, float]:
    if not readme_text:
        return {"web3_relevance": 0.0, "sustainability_relevance": 0.0}
    web3_score = jaccard_similarity(readme_text, WEB3_BEST_PRACTICE_DOC)
    sustainability_score = jaccard_similarity(readme_text, SUSTAINABILITY_BEST_PRACTICE_DOC)
    return {
        "web3_relevance": web3_score,
        "sustainability_relevance": sustainability_score
    }

def calculate_sustainability_score(repo: Dict[str, Any]) -> float:
    stars = repo.get('stargazers_count', 0)
    forks = repo.get('forks_count', 0)
    issues = repo.get('open_issues_count', 0)
    score = (stars * 0.5) + (forks * 0.3) - (issues * 0.2)
    return max(score, 0)

async def fetch_repos_for_keyword(session: aiohttp.ClientSession, keyword: str, page: int) -> List[Dict[str, Any]]:
    params = {
        "q": f"{keyword} in:description,readme,topics",
        "sort": "stars",
        "order": "desc",
        "per_page": 50,
        "page": page
    }
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    async with session.get(GITHUB_API_URL, params=params, headers=headers) as resp:
        if resp.status == 200:
            data = await resp.json()
            return data.get("items", [])
        else:
            logger.warning(f"GitHub fetch error {resp.status} for keyword={keyword}, page={page}")
            return []

async def fetch_readme(session: aiohttp.ClientSession, owner: str, repo: str) -> str:
    url = GITHUB_CONTENTS_API_URL.format(owner=owner, repo=repo)
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    async with session.get(url, headers=headers) as resp:
        if resp.status == 200:
            content_json = await resp.json()
            encoded = content_json.get("content", "")
            try:
                return base64.b64decode(encoded).decode("utf-8")
            except Exception:
                return ""
        return ""

async def gather_github_repos(keywords: List[str]) -> List[Dict[str, Any]]:
    """Gather top repositories for given keywords, compute scores, return list."""
    async with aiohttp.ClientSession() as session:
        seen_ids = set()
        page = 1
        all_repos = []
        # We'll limit total fetches to avoid excessive rate usage
        while page <= 2:  # example: fetch 2 pages for demonstration
            tasks = [fetch_repos_for_keyword(session, kw, page) for kw in keywords]
            results = await asyncio.gather(*tasks)
            for repos_for_kw in results:
                for repo in repos_for_kw:
                    if repo["id"] not in seen_ids:
                        seen_ids.add(repo["id"])
                        owner = repo["owner"]["login"]
                        name = repo["name"]
                        readme_text = await fetch_readme(session, owner, name)
                        readme_scores = calculate_web3_sustainability_scores(readme_text)
                        oss_score = calculate_sustainability_score(repo)
                        all_repos.append({
                            "full_name": repo["full_name"],
                            "description": repo.get("description", ""),
                            "url": repo["html_url"],
                            "stars": repo["stargazers_count"],
                            "forks": repo["forks_count"],
                            "issues": repo["open_issues_count"],
                            "web3_relevance": readme_scores["web3_relevance"],
                            "sustainability_relevance": readme_scores["sustainability_relevance"],
                            "oss_score": oss_score
                        })
            page += 1

        # Normalize & filter
        if not all_repos:
            return []

        max_web3 = max(r["web3_relevance"] for r in all_repos) or 1
        max_sust = max(r["sustainability_relevance"] for r in all_repos) or 1
        max_oss = max(r["oss_score"] for r in all_repos) or 1

        for r in all_repos:
            r["web3_relevance"] /= max_web3
            r["sustainability_relevance"] /= max_sust
            r["oss_score"] /= max_oss

        # Filter out truly irrelevant
        relevant = [
            r for r in all_repos
            if r["web3_relevance"] > 0.01 or r["sustainability_relevance"] > 0.01
        ]
        # Sort by total synergy
        relevant.sort(key=lambda x: x["web3_relevance"] + x["sustainability_relevance"] + x["oss_score"], reverse=True)
        return relevant[:100]

def run_github_scraper(keywords: List[str]) -> List[Dict[str, Any]]:
    """Runs the async GitHub scraper for the given keywords, returns top repos."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    results = loop.run_until_complete(gather_github_repos(keywords))
    loop.close()
    logger.info(f"[GitHub] Found {len(results)} relevant repos.")
    return results

# -------------------------------------------------------------------
# 7) Scraper #4: DeFi Llama
#    Example uses fixed protocols, could also come from Mongo.
# -------------------------------------------------------------------
PROTOCOLS = ["uniswap", "aave", "sushiswap"]  # example
ENDPOINTS_BY_PROTOCOL = {
    "/protocol/{}": "historical_tvl",
    "/tvl/{}": "current_tvl"
}
OPTIONAL_ENDPOINTS_BY_PROTOCOL = {
    "/summary/dexs/{}": "dex_summary",
    "/summary/options/{}": "options_summary",
    "/summary/fees/{}": "fees_summary"
}
REQUEST_DELAY = 1.0

def fetch_endpoint_data(url: str, is_optional=False) -> Any:
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 404 and is_optional:
            logger.warning(f"[DeFiLlama] 404 on optional endpoint: {url}")
            return None
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"[DeFiLlama] Error fetching {url}: {e}")
        return None

def preprocess_defillama_data(data: Any) -> Any:
    """Remove large or unnecessary fields from DeFi Llama data."""
    if isinstance(data, dict):
        for field in ["chainBalances", "tokens", "chainsPrices"]:
            if field in data:
                del data[field]
    elif isinstance(data, list):
        cleaned_list = []
        for item in data:
            if isinstance(item, dict):
                for field in ["chainBalances", "tokens", "chainsPrices"]:
                    item.pop(field, None)
            cleaned_list.append(item)
        data = cleaned_list
    return data

def run_defillama_scraper() -> List[Dict[str, Any]]:
    """
    For each protocol, fetch required + optional endpoints,
    return as list of { protocol, endpoint, data }.
    """
    results = []
    for protocol in PROTOCOLS:
        logger.info(f"[DeFiLlama] Fetching for protocol: {protocol}")
        # Required
        for endpoint_template, name in ENDPOINTS_BY_PROTOCOL.items():
            url = DEFILLAMA_BASE_URL + endpoint_template.format(protocol)
            raw_data = fetch_endpoint_data(url, is_optional=False)
            time.sleep(REQUEST_DELAY)
            if raw_data is not None:
                processed = preprocess_defillama_data(raw_data)
                results.append({
                    "protocol": protocol,
                    "endpoint_name": name,
                    "data": processed
                })
        # Optional
        for endpoint_template, name in OPTIONAL_ENDPOINTS_BY_PROTOCOL.items():
            url = DEFILLAMA_BASE_URL + endpoint_template.format(protocol)
            raw_data = fetch_endpoint_data(url, is_optional=True)
            time.sleep(REQUEST_DELAY)
            if raw_data is not None:
                processed = preprocess_defillama_data(raw_data)
                results.append({
                    "protocol": protocol,
                    "endpoint_name": name,
                    "data": processed
                })
    logger.info(f"[DeFiLlama] Collected {len(results)} endpoint results.")
    return results

# -------------------------------------------------------------------
# 8) Store in Neo4j
#    Each scraper returns data as a list of dicts. We'll store them
#    with different node labels and minimal relationships. 
#    Feel free to adapt the Cypher as needed.
# -------------------------------------------------------------------
def store_reddit_data_in_neo4j(posts: List[Dict[str, Any]]):
    """Create (:RedditPost) nodes."""
    with driver.session() as session:
        for post in posts:
            session.write_transaction(_merge_reddit_post, post)

def _merge_reddit_post(tx, post: Dict[str, Any]):
    query = """
    MERGE (rp:RedditPost { url: $url })
    ON CREATE SET
      rp.subreddit = $subreddit,
      rp.query = $query,
      rp.title = $title,
      rp.text = $text,
      rp.author = $author,
      rp.upvotes = $upvotes,
      rp.comments = $comments,
      rp.timestamp = $timestamp,
      rp.createdAt = timestamp()
    """
    tx.run(query, **post)

def store_news_data_in_neo4j(articles: List[Dict[str, Any]]):
    """Create (:NewsArticle) nodes, keyed by URL."""
    with driver.session() as session:
        for art in articles:
            session.write_transaction(_merge_news_article, art)

def _merge_news_article(tx, art: Dict[str, Any]):
    query = """
    MERGE (na:NewsArticle { url: $url })
    ON CREATE SET
      na.company = $company,
      na.title = $title,
      na.description = $description,
      na.snippet = $snippet,
      na.image_url = $image_url,
      na.published_at = $published_at,
      na.source = $source,
      na.categories = $categories,
      na.createdAt = timestamp()
    """
    # Convert list to a string if needed
    categories = art.get("categories", [])
    if isinstance(categories, list):
        categories = ", ".join(categories)
    params = {
        "url": art.get("url", "unknown"),
        "company": art.get("company"),
        "title": art.get("title"),
        "description": art.get("description"),
        "snippet": art.get("snippet"),
        "image_url": art.get("image_url"),
        "published_at": art.get("published_at"),
        "source": art.get("source"),
        "categories": categories
    }
    tx.run(query, **params)

def store_github_data_in_neo4j(repos: List[Dict[str, Any]]):
    """Create (:GitHubRepo) nodes for each repository."""
    with driver.session() as session:
        for repo in repos:
            session.write_transaction(_merge_github_repo, repo)

def _merge_github_repo(tx, repo: Dict[str, Any]):
    query = """
    MERGE (gh:GitHubRepo { full_name: $full_name })
    ON CREATE SET
      gh.description = $description,
      gh.url = $url,
      gh.stars = $stars,
      gh.forks = $forks,
      gh.issues = $issues,
      gh.web3_relevance = $web3_relevance,
      gh.sustainability_relevance = $sustainability_relevance,
      gh.oss_score = $oss_score,
      gh.createdAt = timestamp()
    """
    tx.run(query, **repo)

def store_defillama_data_in_neo4j(results: List[Dict[str, Any]]):
    """Create (:Protocol) nodes and related (:EndpointData) subnodes."""
    with driver.session() as session:
        for item in results:
            session.write_transaction(_merge_protocol_and_data, item)

def _merge_protocol_and_data(tx, item: Dict[str, Any]):
    protocol_name = item["protocol"]
    endpoint_name = item["endpoint_name"]
    data_json = json.dumps(item["data"])
    # Merge protocol node
    tx.run("""
        MERGE (p:Protocol { name: $protocol_name })
        ON CREATE SET p.createdAt = timestamp()
    """, protocol_name=protocol_name)
    # Merge endpoint data
    tx.run("""
        MERGE (e:EndpointData { endpoint: $endpoint_name, protocol: $protocol_name })
        ON CREATE SET e.createdAt = timestamp()
        SET e.data = $data_json
    """, endpoint_name=endpoint_name, protocol_name=protocol_name, data_json=data_json)
    # Relationship
    tx.run("""
        MATCH (p:Protocol { name: $protocol_name })
        MATCH (e:EndpointData { endpoint: $endpoint_name, protocol: $protocol_name })
        MERGE (p)-[r:HAS_ENDPOINT_DATA]->(e)
        ON CREATE SET r.createdAt = timestamp()
    """, protocol_name=protocol_name, endpoint_name=endpoint_name)

# -------------------------------------------------------------------
# 9) Main entry point
# -------------------------------------------------------------------
def main():
    start_time = time.time()
    logger.info("=== Starting multi-scraper workflow ===")

    # ---------------------------
    # 1) Reddit Scraper
    # ---------------------------
    reddit_posts = run_reddit_scraper()
    store_reddit_data_in_neo4j(reddit_posts)

    # ---------------------------
    # 2) News API Scraper
    #    Fetch companies from Mongo
    # ---------------------------
    companies = get_companies_from_mongo()
    news_articles = run_news_scraper(companies)
    store_news_data_in_neo4j(news_articles)

    # ---------------------------
    # 3) GitHub Scraper
    #    We can either search for "web3" keywords
    #    or also search for your 'companies' if you want.
    # ---------------------------
    github_repos = run_github_scraper(WEB3_KEYWORDS)  # or companies
    store_github_data_in_neo4j(github_repos)

    # ---------------------------
    # 4) DeFi Llama
    #    For a set of protocols
    # ---------------------------
    defillama_results = run_defillama_scraper()
    store_defillama_data_in_neo4j(defillama_results)

    logger.info("=== All scrapers finished. Data stored in Neo4j. ===")
    logger.info(f"Total execution time: {time.time() - start_time:.2f}s")

if __name__ == "__main__":
    main()
