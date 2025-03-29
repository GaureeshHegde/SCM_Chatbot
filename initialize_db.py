# initialize_db.py
from src.database.connection import DatabaseManager

def setup_database(csv_path='src/data/DataCoSupplyChainDataset.csv'):
    """
    Setup database by importing CSV data
    
    :param csv_path: Path to the CSV file
    """
    try:
        db = DatabaseManager()
        db.import_csv(csv_path)
        print("Database setup complete!")
    except Exception as e:
        print(f"Database setup failed: {e}")

if __name__ == "__main__":
    setup_database()