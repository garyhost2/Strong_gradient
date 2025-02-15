import aiohttp
import asyncio
import json
import time
import base64
from aiohttp import ClientSession
import os
from flask import Flask, jsonify, request
from dotenv import load_dotenv
load_dotenv()
# Define Web3 and sustainability-related keywords for filtering
WEB3_KEYWORDS = ['web3', 'ethereum', 'blockchain', 'cryptocurrency', 'defi', 'nft', 'dapp', 'dao', 'smartcontract', 'zkp', 'polkadot', 'ipfs', 'cosmos', 'decentralized', 'cryptography']
SUSTAINABILITY_KEYWORDS = ['sustainability', 'energy', 'carbon', 'environment', 'efficiency', 'renewable', 'green', 'eco', 'offset', 'proof-of-stake', 'energy-efficient', 'carbon-footprint', 'low-carbon', 'eco-friendly']

# GitHub API URL and token
GITHUB_API_URL = 'https://api.github.com/search/repositories'
GITHUB_CONTENTS_API_URL = 'https://api.github.com/repos/{owner}/{repo}/contents/README.md'
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")  
# Updated Web3 Best Practices Document with essential keywords
WEB3_BEST_PRACTICE_DOC = """
web3, ethereum, blockchain, decentralization, smart contract, defi, dapp, dao, ipfs, zkp, polkadot, cosmos, cryptography, trustless, transparency
"""

# Updated Sustainability Best Practices Document with essential keywords
SUSTAINABILITY_BEST_PRACTICE_DOC = """
sustainability, blockchain, proof-of-stake, energy efficiency, carbon offset, renewable energy, low-carbon, eco-friendly, decentralized governance, energy consumption, carbon footprint
"""

# Function to compute Jaccard Similarity (set-based comparison)
def jaccard_similarity(str1, str2):
    set1 = set(str1.lower().split())
    set2 = set(str2.lower().split())
    intersection = set1.intersection(set2)
    union = set1.union(set2)
    return len(intersection) / len(union) if union else 0

# Calculate the relevance score for Web3 and Sustainability Best Practices
def calculate_score_from_text(readme_text):
    # Compute Jaccard similarity for Web3 and Sustainability relevance
    web3_score = jaccard_similarity(readme_text, WEB3_BEST_PRACTICE_DOC)
    sustainability_score = jaccard_similarity(readme_text, SUSTAINABILITY_BEST_PRACTICE_DOC)
    return {
        'web3_relevance': web3_score,
        'sustainability_relevance': sustainability_score
    }

# Sustainability Score Calculation (opensource-related metrics)
def calculate_sustainability_score(repository):
    stars = repository.get('stargazers_count', 0)
    forks = repository.get('forks_count', 0)
    issues = repository.get('open_issues_count', 0)
    
    # Score formula: higher stars and forks increase the score, but open issues decrease it
    score = (stars * 0.5) + (forks * 0.3) - (issues * 0.2)
    return max(score, 0)  # Ensure the score doesn't go negative

# Normalize the scores to the range [0, 1]
def normalize_scores(scores, max_scores):
    return {key: score / max_scores.get(key, 1) for key, score in scores.items()}

# Asynchronous function to get repositories from GitHub based on the Web3 keywords
async def fetch_repositories(session: ClientSession, keyword: str, page: int):
    params = {
        'q': f'{keyword} in:description,readme,topics',
        'sort': 'stars',  # Sort by stars to get the most popular ones first
        'order': 'desc',
        'per_page': 50,  # Request 100 repositories per page
        'page': page  # Pagination, to fetch subsequent pages
    }

    headers = {'Authorization': f'token {GITHUB_TOKEN}'}
    
    async with session.get(GITHUB_API_URL, params=params, headers=headers) as response:
        if response.status == 200:
            data = await response.json()
            return data['items']
        else:
            print(f"Error fetching data for {keyword}: {response.status}")
            return []

# Asynchronous function to fetch the README content (markdown)
async def fetch_readme(session: ClientSession, owner: str, repo: str):
    url = GITHUB_CONTENTS_API_URL.format(owner=owner, repo=repo)
    headers = {'Authorization': f'token {GITHUB_TOKEN}'}
    
    async with session.get(url, headers=headers) as response:
        if response.status == 200:
            content = await response.json()
            # Decode the base64-encoded content and return as markdown
            readme_content = content.get('content', '')
            decoded_readme = base64.b64decode(readme_content).decode('utf-8')
            return decoded_readme
        else:
            print(f"Error fetching README for {owner}/{repo}: {response.status}")
            return None

# Function to process and score repositories
async def process_repositories(keywords, requester_id):
    async with aiohttp.ClientSession() as session:
        # Manage the state of fetched repositories
        seen_repositories = set()  # Track repositories that have already been fetched
        page = 1  # Start from the first page
        repositories_with_scores = []

        while True:
            # Fetch repositories for each Web3-related keyword
            tasks = [fetch_repositories(session, keyword, page) for keyword in keywords]
            results = await asyncio.gather(*tasks)

            # Flatten the results and calculate scores
            max_scores = {
                'web3_relevance': 0,
                'sustainability_relevance': 0,
                'score': 0
            }

            # Calculate scores and track max values for normalization
            for keyword, repos in zip(keywords, results):
                for repo in repos:
                    if repo['id'] not in seen_repositories:  # Ensure we haven't seen this repo before
                        seen_repositories.add(repo['id'])
                        score = calculate_sustainability_score(repo)
                        owner = repo['owner']['login']
                        repo_name = repo['name']
                        
                        # Fetch README markdown content
                        readme_content = await fetch_readme(session, owner, repo_name)
                        
                        # Calculate relevance scores for Web3 and Sustainability
                        readme_scores = calculate_score_from_text(readme_content) if readme_content else {
                            'web3_relevance': 0, 'sustainability_relevance': 0
                        }

                        # Update max scores for normalization
                        max_scores['web3_relevance'] = max(max_scores['web3_relevance'], readme_scores['web3_relevance'])
                        max_scores['sustainability_relevance'] = max(max_scores['sustainability_relevance'], readme_scores['sustainability_relevance'])
                        max_scores['score'] = max(max_scores['score'], score)

                        # Append repository with normalized scores
                        repositories_with_scores.append({
                            'repository_name': repo['full_name'],
                            'score': score,
                            'description': repo.get('description', 'No description'),
                            'url': repo['html_url'],
                            'stars': repo['stargazers_count'],
                            'forks': repo['forks_count'],
                            'issues': repo['open_issues_count'],
                            'web3_relevance': readme_scores['web3_relevance'],
                            'sustainability_relevance': readme_scores['sustainability_relevance']
                        })

            # Check if we fetched fewer than 100 results; if so, stop fetching
            if len(repositories_with_scores) < 100:
                break

            page += 1  # Increment to the next page
        
        # Normalize all scores
        normalized_repositories = []
        for repo in repositories_with_scores:
            normalized_repo = repo.copy()
            normalized_repo['web3_relevance'] = normalized_repo['web3_relevance'] / max_scores['web3_relevance']
            normalized_repo['sustainability_relevance'] = normalized_repo['sustainability_relevance'] / max_scores['sustainability_relevance']
            normalized_repo['score'] = normalized_repo['score'] / max_scores['score']
            normalized_repositories.append(normalized_repo)

        # Filter repositories that are relevant enough (e.g., Web3 or Sustainability relevance > 0.01)
        relevant_repositories = [repo for repo in normalized_repositories if repo['web3_relevance'] > 0.01 or repo['sustainability_relevance'] > 0.01]

        # Sort repositories by total score (sustainability score + relevance scores)
        relevant_repositories.sort(key=lambda x: (x['score'] + x['web3_relevance'] + x['sustainability_relevance']), reverse=True)
        return relevant_repositories[:100]  # Return top 100 repositories

# Flask API setup
app = Flask(__name__)

# Route to handle the Web3 sustainability search and return results in JSON
@app.route('/api/sustainability', methods=['GET'])
def get_sustainability():
    keywords = request.args.get('keywords', default=','.join(WEB3_KEYWORDS), type=str)
    keywords = keywords.split(',')
    requester_id = request.args.get('requester_id', type=str)  # Assume requester_id is provided

    start_time = time.time()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    top_repositories = loop.run_until_complete(process_repositories(keywords, requester_id))
    
    # Return the response as JSON
    response = {
        'top_repositories': top_repositories,
        'execution_time': time.time() - start_time
    }
    return jsonify(response)

# Main function to run the Flask app
if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)
