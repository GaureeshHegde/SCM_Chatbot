import os
import sqlite3
import json
import ollama
from datetime import datetime
from typing import List, Dict, Union

class SupplyChainDB:
    """Handles database setup and LLM-powered querying in one class"""
    
    def __init__(self, db_path: str = 'supply_chain.db', csv_path: str = None, model: str = 'codellama:7b-instruct'):
        """
        Initialize database and LLM translator
        
        :param db_path: Path to SQLite database file
        :param csv_path: Path to CSV data file (optional, only for initial setup)
        :param model: Ollama model name for query translation
        """
        self.db_path = db_path
        self.model = model
        
        # Initialize database if needed
        if csv_path and not os.path.exists(self.db_path):
            self._initialize_db(csv_path)
            
        # Verify database is ready
        self._verify_db()

    def _initialize_db(self, csv_path: str):
        """Create database from CSV"""
        import pandas as pd
        from sqlalchemy import create_engine
        
        # Read and clean CSV
        df = pd.read_csv(csv_path, parse_dates=['order date (DateOrders)', 'shipping date (DateOrders)'])
        df.columns = [col.lower().replace(' ', '_').replace('(', '').replace(')', '') for col in df.columns]
        
        # Create SQLite database
        engine = create_engine(f'sqlite:///{self.db_path}')
        df.to_sql('supply_chain_orders', engine, index=False, if_exists='replace')
        print(f"Database created at {self.db_path} with {len(df)} records")

    def _verify_db(self):
        """Check if database is accessible"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.execute("SELECT 1 FROM supply_chain_orders LIMIT 1")
            conn.close()
        except Exception as e:
            raise RuntimeError(f"Database verification failed: {str(e)}")

    def query(self, natural_query: str) -> Dict[str, Union[str, List[Dict]]]:
        """
        Execute natural language query against database
        
        :param natural_query: Query in plain English
        :return: Dictionary with 'query' (SQL) and 'results' (data)
        """
        try:
            # Get schema context
            schema = self._get_schema()
            
            # Generate SQL with LLM
            prompt = self._create_prompt(natural_query, schema)
            response = ollama.chat(
                model=self.model,
                messages=[{'role': 'user', 'content': prompt}],
                options={'temperature': 0.1}
            )
            sql = response['message']['content'].strip().strip(';')
            
            # Execute and return
            return {
                'query': sql,
                'results': self._execute_sql(sql)
            }
        except Exception as e:
            return {
                'error': str(e),
                'query': None,
                'results': None
            }

    def _get_schema(self) -> Dict:
        """Extract schema with sample data"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get columns
        cursor.execute("PRAGMA table_info(supply_chain_orders)")
        columns = [{'name': col[1], 'type': col[2]} for col in cursor.fetchall()]
        
        # Get samples
        cursor.execute("SELECT * FROM supply_chain_orders LIMIT 3")
        samples = [dict(zip([col[0] for col in cursor.description], row)) 
                  for row in cursor.fetchall()]
        
        conn.close()
        return {'columns': columns, 'samples': samples}

    def _create_prompt(self, query: str, schema: Dict) -> str:
        """Generate precise LLM prompt"""
        return f"""
Convert this supply chain query to SQLite SQL:
"{query}"

Database Schema:
{json.dumps(schema['columns'], indent=2)}

Sample Rows:
{json.dumps(schema['samples'], indent=2)}

Rules:
1. Use EXACT column names from schema
2. For dates: Use 'YYYY-MM-DD' format
3. For locations: Use full names ('Puerto Rico' not 'PR')
4. Return ONLY the SQL query, no commentary

SQL Query:"""

    def _execute_sql(self, sql: str) -> List[Dict]:
        """Execute SQL safely"""
        if not self._validate_sql(sql):
            raise ValueError("Query failed safety check")
            
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(sql)
        
        columns = [col[0] for col in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        conn.close()
        return results

    def _validate_sql(self, sql: str) -> bool:
        """Basic SQL injection protection"""
        forbidden = [';', '--', '/*', '*/', 'drop ', 'delete ', 'update ', 'insert ']
        return not any(f in sql.lower() for f in forbidden)


# Example Usage
if __name__ == "__main__":
    # First-time setup (uncomment to run once)
    # db = SupplyChainDB(csv_path='your_data.csv')
    
    # Normal querying
    db = SupplyChainDB()  # Reuse existing DB
    
    # Sample queries to try:
    print(db.query("Show 5 orders from Puerto Rico in January 2018"))
    print(db.query("Which product categories have the most late deliveries?"))
    print(db.query("List orders with sales over $500"))