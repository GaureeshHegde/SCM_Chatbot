from typing import Dict, List, Union
from sqlalchemy.orm import Session
from src.database.connection import DatabaseManager
from src.models.query_translator import OllamaQueryTranslator

class SupplyChainAgent:
    """
    Orchestrates query processing from natural language to database results.
    Handles:
    - Query translation
    - Database operations
    - Response formatting
    - Pagination
    - Query validation
    """

    def __init__(self, db_path: str = 'supply_chain.db'):
        """
        Initialize with database connection and query translator.
        
        :param db_path: Path to SQLite database file
        """
        self.db_manager = DatabaseManager(db_path)
        self.session = self.db_manager.get_session()
        self.translator = OllamaQueryTranslator(self.session)
        
        # Supply chain related keywords for basic validation
        self.supply_chain_keywords = [
            "order", "delivery", "product", "supplier", "inventory", 
            "shipment", "warehouse", "customer", "logistics", "stock",
            "supply", "chain", "transport", "shipping", "purchase"
        ]

    def _validate_query(self, user_query: str) -> bool:
        """
        Basic validation to check if query is relevant to supply chain.
        
        :param user_query: Natural language query string
        :return: Boolean indicating query validity
        """
        query_lower = user_query.lower()
        return any(keyword in query_lower for keyword in self.supply_chain_keywords)

    def handle_query(self, user_query: str, limit: int = 10, offset: int = 0) -> Dict[str, Union[str, List, int]]:
        """
        End-to-end query processing pipeline with pagination.
        
        :param user_query: Natural language query string
        :param limit: Maximum number of records to return
        :param offset: Number of records to skip
        :return: Dictionary with 'response' and metadata
        """
        try:
            # Step 1: Validate query
            if not self._validate_query(user_query):
                return {
                    "response": "Your query doesn't seem related to supply chain data. Please rephrase with relevant terms.",
                    "status": "invalid"
                }
            
            # Step 2: Translate to executable query
            translation = self.translator.translate_query(user_query, limit=limit, offset=offset)
            
            # Step 3: Calculate pagination metadata
            total_count = translation.get('total_count', len(translation['results']))
            
            # Step 4: Format for user presentation
            return {
                "response": self._format_response(translation['results']),
                "query_used": translation['query'],  # For debugging
                "status": "success",
                "pagination": {
                    "limit": limit,
                    "offset": offset,
                    "total": total_count,
                    "has_more": offset + len(translation['results']) < total_count
                }
            }
            
        except Exception as e:
            return {
                "response": f"Error processing query: {str(e)}",
                "status": "error"
            }

    def _format_response(self, results: List[Dict]) -> str:
        """
        Convert raw database results to human-readable format.
        
        :param results: List of database records as dictionaries
        :return: Formatted string response
        """
        if not results:
            return "No matching records found."
        
        samples = results[:3] if len(results) > 3 else results
        formatted_samples = "\n\n".join(f"Record {i+1}:\n{self._dict_to_bullets(sample)}" 
                                      for i, sample in enumerate(samples))
        
        return (
            f"Found {len(results)} records.\n\n"
            f"Sample results:\n{formatted_samples}"
        )

    def _dict_to_bullets(self, data: Dict) -> str:
        """Helper for dictionary formatting"""
        return "\n".join(f"- {k}: {v}" for k, v in data.items() if v is not None)

    def close(self):
        """Clean up resources"""
        self.session.close()


# Example Usage
if __name__ == "__main__":
    agent = SupplyChainAgent()
    
    try:
        # Test queries with pagination
        queries = [
            "Show 5 orders from Puerto Rico",
            "List late deliveries with high risk",
            "What products had highest sales last month?"
        ]
        
        for query in queries:
            print(f"\nQuery: {query}")
            # First page
            response = agent.handle_query(query, limit=5, offset=0)
            print(response['response'])
            
            # Simulate pagination if there are more results
            if response.get('pagination', {}).get('has_more', False):
                print("\nShowing next page...")
                next_response = agent.handle_query(query, limit=5, offset=5)
                print(next_response['response'])
            
    finally:
        agent.close()