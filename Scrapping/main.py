import os
import sys
import time
import json
import logging
from typing import Any, Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed
import asyncio
import aiohttp
import base64
import requests
from dotenv import load_dotenv
# from pymongo import MongoClient  # REMOVED: no longer needed
from neo4j import GraphDatabase, basic_auth
import praw

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

# --- MIDDLEWARE BASE URL (NGROK, etc.) ---
NGROK_BASE_URL = "https://b489-196-203-181-122.ngrok-free.app"

# Removed Mongo
# MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
# MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "mydb")
# MONGO_COLLECTION = os.getenv("MONGO_COMPANIES_COLLECTION", "companies")

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT")

NEWS_API_TOKEN = os.getenv("NEWS_API_TOKEN")
NEWS_API_BASE_URL = "https://api.thenewsapi.com/v1/news/all"

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
GITHUB_API_URL = 'https://api.github.com/search/repositories'
GITHUB_CONTENTS_API_URL = 'https://api.github.com/repos/{owner}/{repo}/contents/README.md'

DEFILLAMA_BASE_URL = "https://api.llama.fi"

driver = GraphDatabase.driver(NEO4J_URI, auth=basic_auth(NEO4J_USER, NEO4J_PASSWORD))

reddit = praw.Reddit(
    client_id=REDDIT_CLIENT_ID,
    client_secret=REDDIT_CLIENT_SECRET,
    user_agent=REDDIT_USER_AGENT
)

# -----------------------------------------------------------------------------
# UPDATED: Fetch Companies via Middleware instead of MongoDB
# -----------------------------------------------------------------------------
def get_companies_from_mongo() -> List[str]:
    """
    Fetch company names from your middleware (replaces direct MongoDB calls).
    Example endpoint: GET /api/v1/companies
    Returns a list of company names.
    """
    try:
        url = f"{NGROK_BASE_URL}/api/v1/companies"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        # Assuming 'data' is a list of objects that contain "name"
        return [item["name"] for item in data if "name" in item]
    except Exception as e:
        logger.error(f"Failed to retrieve companies via middleware: {e}")
        return []

def update_company_treated(name: str) -> bool:
    """
    Example function to update a company's 'treated' status using your middleware.
    Endpoint: /api/v1/companies/update-treated?name=<company_name>
    """
    try:
        url = f"{NGROK_BASE_URL}/api/v1/companies/update-treated"
        params = {"name": name}
        response = requests.put(url, params=params, timeout=10)
        response.raise_for_status()
        logger.info(f"Company '{name}' treated status updated successfully.")
        return True
    except Exception as e:
        logger.error(f"Failed to update treated status for '{name}': {e}")
        return False

# -----------------------------------------------------------------------------
# Reddit Scraper
# -----------------------------------------------------------------------------
SUBREDDITS = [
    "ethdev", "solana", "web3", "CryptoTechnology", "defi", "CryptoMarkets"
]
QUERIES = [
    "web3", "blockchain", "crypto", "decentralized finance", "smart contract"
]
MAX_REDDIT_POSTS = 50
MAX_WORKERS = 10

def scrape_subreddit(subreddit: str, query: str) -> List[Dict[str, Any]]:
    """Scrape a single subreddit for a given query."""
    results = []
    try:
        for submission in reddit.subreddit(subreddit).search(query, limit=MAX_REDDIT_POSTS):
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
    """Run the Reddit scraper concurrently."""
    all_data = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(scrape_subreddit, subreddit, query)
            for subreddit in SUBREDDITS for query in QUERIES
        ]
        for f in as_completed(futures):
            all_data.extend(f.result())
    logger.info(f"[Reddit] Scraped {len(all_data)} total posts.")
    return all_data

# -----------------------------------------------------------------------------
# News API Scraper
# -----------------------------------------------------------------------------
def fetch_news_for_company(company: str) -> List[Dict[str, Any]]:
    """Fetch news articles for a single company."""
    params = {
        "api_token": NEWS_API_TOKEN,
        "language": "en",
        "search": company,
        "limit": 3,
        "sort": "published_at"
    }
    try:
        resp = requests.get(NEWS_API_BASE_URL, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if "data" not in data:
            return []
        return [
            {
                "company": company,
                "title": article.get("title"),
                "description": article.get("description"),
                "snippet": article.get("snippet"),
                "url": article.get("url"),
                "image_url": article.get("image_url"),
                "published_at": article.get("published_at"),
                "source": article.get("source"),
                "categories": article.get("categories", [])
            }
            for article in data["data"]
        ]
    except Exception as e:
        logger.warning(f"Error fetching news for '{company}': {e}")
        return []

def run_news_scraper(companies: List[str]) -> List[Dict[str, Any]]:
    """Run the News API scraper for a list of companies."""
    all_news = []
    for idx, company in enumerate(companies):
        # Example stopping logic at 90 to avoid potential rate limits
        if idx >= 90:
            logger.info("Reached ~90 requests, stopping to avoid daily limit.")
            break
        results = fetch_news_for_company(company)
        all_news.extend(results)
        time.sleep(2)  # Respect rate-limits or courtesy wait
    logger.info(f"[NewsAPI] Fetched {len(all_news)} news articles.")
    return all_news

# -----------------------------------------------------------------------------
# GitHub Scraper
# -----------------------------------------------------------------------------
WEB3_KEYWORDS = ["web3", "ethereum", "blockchain", "cryptocurrency"]

async def fetch_repos_for_keyword(session: aiohttp.ClientSession, keyword: str, page: int) -> List[Dict[str, Any]]:
    """Fetch repositories for a keyword."""
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
    """Fetch README content for a repository."""
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
    """Gather top repositories for given keywords."""
    async with aiohttp.ClientSession() as session:
        seen_ids = set()
        all_repos = []
        for page in range(1, 3):  # Fetch up to 2 pages per keyword
            tasks = [fetch_repos_for_keyword(session, kw, page) for kw in keywords]
            results = await asyncio.gather(*tasks)
            for repos_for_kw in results:
                for repo in repos_for_kw:
                    if repo["id"] not in seen_ids:
                        seen_ids.add(repo["id"])
                        owner = repo["owner"]["login"]
                        name = repo["name"]
                        readme_text = await fetch_readme(session, owner, name)
                        all_repos.append({
                            "full_name": repo["full_name"],
                            "description": repo.get("description", ""),
                            "url": repo["html_url"],
                            "stars": repo["stargazers_count"],
                            "forks": repo["forks_count"],
                            "issues": repo["open_issues_count"],
                            "readme": readme_text
                        })
    return all_repos

def run_github_scraper(keywords: List[str]) -> List[Dict[str, Any]]:
    """Run the GitHub scraper."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    results = loop.run_until_complete(gather_github_repos(keywords))
    loop.close()
    logger.info(f"[GitHub] Found {len(results)} relevant repos.")
    return results

# -----------------------------------------------------------------------------
# DeFi Llama Scraper
# -----------------------------------------------------------------------------
PROTOCOLS = ["uniswap", "aave", "sushiswap"]

ENDPOINTS_BY_PROTOCOL = {
    "/protocol/{}": "historical_tvl",
    "/tvl/{}": "current_tvl"
}

def fetch_endpoint_data(url: str, is_optional=False) -> Any:
    """Fetch data from a DeFi Llama endpoint."""
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
    """Preprocess DeFi Llama data."""
    if isinstance(data, dict):
        for field in ["chainBalances", "tokens", "chainsPrices"]:
            data.pop(field, None)
    elif isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                for field in ["chainBalances", "tokens", "chainsPrices"]:
                    item.pop(field, None)
    return data

def run_defillama_scraper() -> List[Dict[str, Any]]:
    """Run the DeFi Llama scraper."""
    results = []
    for protocol in PROTOCOLS:
        for endpoint_template, name in ENDPOINTS_BY_PROTOCOL.items():
            url = DEFILLAMA_BASE_URL + endpoint_template.format(protocol)
            raw_data = fetch_endpoint_data(url, is_optional=False)
            if raw_data:
                processed = preprocess_defillama_data(raw_data)
                results.append({
                    "protocol": protocol,
                    "endpoint_name": name,
                    "data": processed
                })
    logger.info(f"[DeFiLlama] Collected {len(results)} endpoint results.")
    return results

# -----------------------------------------------------------------------------
# Neo4j Storage
# -----------------------------------------------------------------------------
def store_reddit_data_in_neo4j(posts: List[Dict[str, Any]]):
    """Store Reddit posts in Neo4j."""
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
    """Store news articles in Neo4j."""
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
    categories = ", ".join(art.get("categories", []))
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
    """Store GitHub repositories in Neo4j."""
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
      gh.readme = $readme,
      gh.createdAt = timestamp()
    """
    tx.run(query, **repo)

def store_defillama_data_in_neo4j(results: List[Dict[str, Any]]):
    """Store DeFi Llama data in Neo4j."""
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
    ON CREATE SET e.createdAt = timestamp(), e.data = $data_json
    """, endpoint_name=endpoint_name, protocol_name=protocol_name, data_json=data_json)

    # Relationship
    tx.run("""
    MATCH (p:Protocol { name: $protocol_name })
    MATCH (e:EndpointData { endpoint: $endpoint_name, protocol: $protocol_name })
    MERGE (p)-[r:HAS_ENDPOINT_DATA]->(e)
    ON CREATE SET r.createdAt = timestamp()
    """, protocol_name=protocol_name, endpoint_name=endpoint_name)

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main():
    start_time = time.time()
    logger.info("=== Starting multi-scraper workflow ===")

    # 1) Reddit Scraper
    reddit_posts = run_reddit_scraper()
    store_reddit_data_in_neo4j(reddit_posts)

    # 2) News API Scraper
    companies = get_companies_from_mongo()  # Now retrieves from middleware
    news_articles = run_news_scraper(companies)
    store_news_data_in_neo4j(news_articles)



    # 3) GitHub Scraper
    github_repos = run_github_scraper(WEB3_KEYWORDS)
    store_github_data_in_neo4j(github_repos)

    # 4) DeFi Llama
    defillama_results = run_defillama_scraper()
    store_defillama_data_in_neo4j(defillama_results)

    logger.info("=== All scrapers finished. Data stored in Neo4j. ===")
    logger.info(f"Total execution time: {time.time() - start_time:.2f}s")

if __name__ == "__main__":
    main()
