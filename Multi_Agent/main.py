import os
import json
import asyncio
import logging
import sys
from typing import Any, Dict, List
from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification
from neo4j import GraphDatabase, basic_auth
from dotenv import load_dotenv
import ollama

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

# Environment variables
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

# Initialize Neo4j driver
driver = GraphDatabase.driver(NEO4J_URI, auth=basic_auth(NEO4J_USER, NEO4J_PASSWORD))

# Initialize CryptoBERT pipeline
pipe = pipeline("text-classification", model="ElKulako/cryptobert")
tokenizer = AutoTokenizer.from_pretrained("ElKulako/cryptobert")
model = AutoModelForSequenceClassification.from_pretrained("ElKulako/cryptobert")

# Graph Querying Module
class Neo4jGraph:
    def __init__(self, driver):
        self.driver = driver

    def run_query(self, query: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Run a Cypher query and return results."""
        with self.driver.session() as session:
            result = session.run(query, params or {})
            return [record.data() for record in result]

    def fetch_context_for_topic(self, topic: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Fetch context related to a specific topic."""
        query = """
        MATCH (n)-[:COVERS_TOPIC]->(t:Topic {name: $topic})
        RETURN n, t LIMIT $limit
        """
        return self.run_query(query, {"topic": topic, "limit": limit})

    def fetch_related_entities(self, entity_type: str, entity_id: str) -> List[Dict[str, Any]]:
        """Fetch entities related to a given node."""
        query = f"""
        MATCH (n:{entity_type} {{id: $entity_id}})-[r]-()
        RETURN type(r) AS relationship, collect(properties(r)) AS relations
        """
        return self.run_query(query, {"entity_id": entity_id})

# Base Agent Class
class BaseAgent:
    def __init__(self, graph: Neo4jGraph):
        self.graph = graph
        self.ollama_client = ollama.AsyncClient()

    def classify_query(self, query: str) -> Dict[str, Any]:
        """Classify the user query using CryptoBERT."""
        result = pipe(query)[0]
        return {
            "label": result["label"],
            "score": result["score"]
        }

    def _format_prompt(self, context: List[Dict[str, Any]], classification: Dict[str, Any]) -> str:
        """Format context and classification into a prompt for LLMs."""
        context_str = "\n".join([json.dumps(item, indent=2) for item in context])
        return f"""User query classification: {classification['label']} (confidence: {classification['score']:.2f})

Relevant context from knowledge graph:
{context_str}

Please provide a comprehensive response integrating information from the context. Be concise but thorough in your analysis."""

    async def generate_response(self, context: List[Dict[str, Any]], classification: Dict[str, Any]) -> str:
        """Generate response using both Qwen 2.5 and DeepSeek R1."""
        if not context:
            return f"No relevant information found for '{classification['label']}'."

        prompt = self._format_prompt(context, classification)
        
        try:
            # Generate responses from both models concurrently
            qwen_task = self.ollama_client.generate(
                model='qwen2.5',
                prompt=prompt,
                options={'temperature': 0.7, 'max_tokens': 1024}
            )
            deepseek_task = self.ollama_client.generate(
                model='deepseek-r1',
                prompt=prompt,
                options={'temperature': 0.7, 'max_tokens': 1024}
            )

            responses = await asyncio.gather(qwen_task, deepseek_task)
            
            combined_response = (
                f"**Qwen 2.5 Analysis**:\n{responses[0]['response']}\n\n"
                f"**DeepSeek R1 Insights**:\n{responses[1]['response']}"
            )
            return combined_response
        except Exception as e:
            logger.error(f"Error generating LLM response: {str(e)}")
            return "An error occurred while generating the analysis. Please try again later."

# Specific Agents
class FinanceAgent(BaseAgent):
    async def handle_query(self, query: str) -> str:
        """Handle finance-related queries."""
        classification = self.classify_query(query)
        context = await asyncio.to_thread(
            self.graph.fetch_context_for_topic, 
            topic="finance", 
            limit=5
        )
        return await self.generate_response(context, classification)

class Web3DevelopmentAgent(BaseAgent):
    async def handle_query(self, query: str) -> str:
        """Handle web3 development-related queries."""
        classification = self.classify_query(query)
        context = await asyncio.to_thread(
            self.graph.fetch_context_for_topic,
            topic="web3",
            limit=5
        )
        return await self.generate_response(context, classification)

class SustainabilityAgent(BaseAgent):
    async def handle_query(self, query: str) -> str:
        """Handle sustainability-related queries."""
        classification = self.classify_query(query)
        context = await asyncio.to_thread(
            self.graph.fetch_context_for_topic,
            topic="sustainability",
            limit=5
        )
        return await self.generate_response(context, classification)

class GeneralKnowledgeAgent(BaseAgent):
    async def handle_query(self, query: str) -> str:
        """Handle general knowledge queries."""
        classification = self.classify_query(query)
        context = await asyncio.to_thread(
            self.graph.fetch_context_for_topic,
            topic="general",
            limit=5
        )
        return await self.generate_response(context, classification)

# Query Router (unchanged)
class QueryRouter:
    def __init__(self, agents: Dict[str, BaseAgent]):
        self.agents = agents

    def route_query(self, query: str) -> List[BaseAgent]:
        """Route the query to the appropriate agent(s)."""
        if "protocol" in query.lower() or "tvl" in query.lower():
            return [self.agents["finance"]]
        elif "github" in query.lower() or "repository" in query.lower():
            return [self.agents["web3_development"]]
        elif "sustainability" in query.lower() or "green" in query.lower():
            return [self.agents["sustainability"]]
        else:
            return [self.agents["general_knowledge"]]

# Main Workflow
async def main():
    logger.info("=== Starting Multi-Agent Graph RAG System ===")

    # Initialize Neo4j graph
    graph = Neo4jGraph(driver)

    # Initialize agents
    agents = {
        "finance": FinanceAgent(graph),
        "web3_development": Web3DevelopmentAgent(graph),
        "sustainability": SustainabilityAgent(graph),
        "general_knowledge": GeneralKnowledgeAgent(graph),
    }

    # Initialize query router
    router = QueryRouter(agents)

    
    user_query = "What are the top sustainable blockchain projects?"
    selected_agents = router.route_query(user_query)

    # Generate responses concurrently
    responses = await asyncio.gather(
        *[agent.handle_query(user_query) for agent in selected_agents]
    )

    # Aggregate responses
    final_response = "\n\n".join(responses)
    logger.info(f"Final Response:\n{final_response}")

if __name__ == "__main__":
    asyncio.run(main())