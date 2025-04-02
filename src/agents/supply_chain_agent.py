from typing import Dict, List, Union
from sqlalchemy.orm import Session
from src.database.connection import DatabaseManager
from src.models.query_translator import OllamaQueryTranslator

class SupplyChainAgent:
    """
    Orchestrates end-to-end natural language query processing with:
    - Query validation
    - Translation to SQL
    - Paginated execution
    - Result formatting
    """

    def __init__(self, db_path: str = 'supply_chain.db'):
        """
        Initialize with database connection
        
        :param db_path: Path to SQLite database file
        """
        self.db_manager = DatabaseManager(db_path)
        self.session = self.db_manager.get_session()
        self.translator = OllamaQueryTranslator(self.session)
        
        # Supply chain domain keywords for validation
        self.domain_keywords = [
            'order', 'shipment', 'inventory', 'supplier', 
            'customer', 'delivery', 'product', 'warehouse',
            'logistics', 'purchase', 'stock', 'shipping'
        ]

    def handle_query(self, 
                   user_query: str, 
                   limit: int = 10, 
                   offset: int = 0) -> Dict[str, Union[str, List, Dict]]:
        """
        Process natural language query with pagination
        
        :param user_query: Natural language question
        :param limit: Results per page
        :param offset: Pagination offset
        :return: {
            'response': str, 
            'query_used': str,
            'status': str,
            'pagination': dict
        }
        """
        try:
            # Step 1: Validate query relevance
            if not self._is_valid_query(user_query):
                return {
                    'response': 'Please ask supply chain related questions (e.g., orders, shipments)',
                    'status': 'invalid'
                }

            # Step 2: Translate and execute
            result = self.translator.translate_query(user_query, limit, offset)
            
            if 'error' in result:
                raise ValueError(result['error'])

            # Step 3: Format response
            return {
                'response': self._format_results(result['results']),
                'query_used': result['query'],
                'status': 'success',
                'pagination': {
                    'limit': limit,
                    'offset': offset,
                    'total': result.get('total_count', len(result['results'])),
                    'has_more': (offset + limit) < result.get('total_count', float('inf'))
                }
            }

        except Exception as e:
            return {
                'response': f"Error: {str(e)}",
                'status': 'error'
            }

    def _is_valid_query(self, query: str) -> bool:
        """Check if query is supply chain related"""
        query_lower = query.lower()
        return any(keyword in query_lower for keyword in self.domain_keywords)

    def _format_results(self, results: List[Dict]) -> str:
        """
        Convert raw results to human-readable format
        
        :param results: List of database rows as dicts
        :return: Formatted string with samples
        """
        if not results:
            return "No matching records found."
        
        # Display first 3 results as samples
        sample_size = min(3, len(results))
        samples = "\n\n".join(
            f"Result {i+1}:\n" + "\n".join(f"- {k}: {v}" for k, v in row.items())
            for i, row in enumerate(results[:sample_size])
        )

        return (
            f"Found {len(results)} records\n\n"
            f"Sample results:\n{samples}"
        )

    def close(self):
        """Clean up database resources"""
        self.session.close()
        self.db_manager.engine.dispose()


# Example Usage
if __name__ == "__main__":
    agent = SupplyChainAgent()
    
    try:
        # Test basic query
        print(agent.handle_query("Show 5 recent orders"))
        
        # Test pagination
        page1 = agent.handle_query("List shipments", limit=5, offset=0)
        print(f"Page 1: {page1['response']}")
        
        if page1['pagination']['has_more']:
            page2 = agent.handle_query("List shipments", limit=5, offset=5)
            print(f"Page 2: {page2['response']}")
            
    finally:
        agent.close()