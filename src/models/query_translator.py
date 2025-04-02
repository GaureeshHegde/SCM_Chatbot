# src/models/query_translator.py
import json
import ollama
from typing import List, Dict, Union
from sqlalchemy.orm import Session
from sqlalchemy import inspect
import sqlite3
class OllamaQueryTranslator:
    """
    Translates natural language queries to SQL and executes them using SQLAlchemy session
    """
    
    def __init__(self, session: Session):
        """
        Initialize with SQLAlchemy session
        
        :param session: SQLAlchemy database session from DatabaseManager
        """
        self.session = session
        self.model = 'codellama:7b-instruct'

    def translate_query(self, query: str, limit: int = None, offset: int = None) -> Dict[str, Union[str, List[Dict]]]:
        """
        Main translation method with pagination support
        
        :param query: Natural language query
        :param limit: Maximum results to return
        :param offset: Results offset for pagination
        :return: Dictionary with 'query', 'results', and 'total_count'
        """
        try:
            # Get schema context
            schema = self._get_schema()
            
            # Generate SQL with Ollama
            sql = self._generate_sql(query, schema, limit, offset)
            
            # Execute and return
            return {
                'query': sql,
                'results': self._execute_sql(sql),
                'total_count': self._get_total_count(sql)  # For pagination
            }
        except Exception as e:
            return {
                'error': str(e),
                'query': None,
                'results': None
            }

    def _get_schema(self) -> Dict:
        """Extract schema information using SQLAlchemy"""
        inspector = inspect(self.session.bind)
        columns = inspector.get_columns('supply_chain_orders')
        sample = self.session.execute(
            "SELECT * FROM supply_chain_orders LIMIT 3"
        ).fetchall()
        
        return {
            'columns': [{'name': col['name'], 'type': str(col['type'])} for col in columns],
            'samples': [dict(row) for row in sample]
        }

    def _generate_sql(self, query: str, schema: Dict, limit: int, offset: int) -> str:
        """Generate SQL using Ollama with pagination hints"""
        prompt = f"""
Convert this supply chain query to SQL:
"{query}"

Schema:
{json.dumps(schema['columns'], indent=2)}

Sample Data:
{json.dumps(schema['samples'], indent=2)}

Rules:
1. Use exact column names
2. Add LIMIT {limit} OFFSET {offset} for pagination
3. Return ONLY SQL
        """
        response = ollama.chat(
            model=self.model,
            messages=[{'role': 'user', 'content': prompt}],
            options={'temperature': 0.1}
        )
        return response['message']['content'].strip().strip(';')

from sqlalchemy import text

def _execute_sql(self, sql: str) -> List[Dict]:
    """Execute SQL safely"""
    if not self._validate_sql(sql):
        raise ValueError("Query failed safety check")
        
    conn = sqlite3.connect(self.db_path)
    cursor = conn.cursor()
    
    # Explicitly use text() wrapper
    cursor.execute(text(sql))  # <-- Fix applied

    columns = [col[0] for col in cursor.description]
    results = [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    conn.close()
    return results

    def _get_total_count(self, sql: str) -> int:
        """Get total results count for pagination"""
        try:
            count_sql = f"SELECT COUNT(*) FROM ({sql}) AS subquery"
            return self.session.execute(text(count_sql)).scalar()
        except Exception as e:
            raise ValueError(f"Count query failed: {str(e)}")

    def _validate_sql(self, sql: str) -> bool:
        """Basic SQL injection protection"""
        forbidden = [';', '--', '/*', '*/', 'drop ', 'delete ', 'update ', 'insert ']
        return not any(f in sql.lower() for f in forbidden)