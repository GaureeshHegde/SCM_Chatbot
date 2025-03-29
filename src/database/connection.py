import os
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class SupplyChainOrder(Base):
    """SQLAlchemy model matching your CSV structure"""
    __tablename__ = 'supply_chain_orders'
    
    # (Keep all your existing column definitions here)
    id = Column(Integer, primary_key=True, autoincrement=True)
    type = Column(String)
    days_for_shipping_real = Column(Integer)
    days_for_shipment_scheduled = Column(Integer)
    benefit_per_order = Column(Float)
    sales_per_customer = Column(Float)
    delivery_status = Column(String)
    late_delivery_risk = Column(Integer)
    category_id = Column(Integer)
    category_name = Column(String)
    customer_city = Column(String)
    customer_country = Column(String)
    customer_email = Column(String)
    customer_fname = Column(String)
    customer_id = Column(String)
    customer_lname = Column(String)
    customer_password = Column(String)
    customer_segment = Column(String)
    customer_state = Column(String)
    customer_street = Column(String)
    customer_zipcode = Column(String)
    department_id = Column(Integer)
    department_name = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    market = Column(String)
    order_city = Column(String)
    order_country = Column(String)
    order_customer_id = Column(String)
    order_date = Column(Date)
    order_id = Column(String)
    order_item_cardprod_id = Column(Integer)
    order_item_discount = Column(Float)
    order_item_discount_rate = Column(Float)
    order_item_id = Column(Integer)
    order_item_product_price = Column(Float)
    order_item_profit_ratio = Column(Float)
    order_item_quantity = Column(Integer)
    sales = Column(Float)
    order_item_total = Column(Float)
    order_profit_per_order = Column(Float)
    order_region = Column(String)
    order_state = Column(String)
    order_status = Column(String)
    order_zipcode = Column(String)
    product_card_id = Column(Integer)
    product_category_id = Column(Integer)
    product_description = Column(String)
    product_image = Column(String)
    product_name = Column(String)
    product_price = Column(Float)
    product_status = Column(Integer)
    shipping_date = Column(Date)
    shipping_mode = Column(String)

class DatabaseManager:
    def __init__(self, db_path='supply_chain.db', default_csv_path=None):
        """
        Initialize with optional default paths.
        
        :param db_path: Path to SQLite database file
        :param default_csv_path: Default CSV path if none provided to import_csv()
        """
        self.db_path = os.path.abspath(db_path)
        self.default_csv_path = default_csv_path
        self.engine = create_engine(f'sqlite:///{self.db_path}')
        Base.metadata.create_all(self.engine)
        self.SessionLocal = sessionmaker(bind=self.engine)

    def import_csv(self, csv_path=None):
        """
        Import CSV data with robust error handling.
        
        :param csv_path: Path to CSV file (uses default if None)
        """
        csv_path = csv_path or self.default_csv_path
        if not csv_path:
            raise ValueError("No CSV path provided and no default set")
        
        csv_path = os.path.abspath(csv_path)
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found at: {csv_path}")

        try:
            # Try multiple encodings
            for encoding in ['utf-8', 'latin1', 'utf-16']:
                try:
                    df = pd.read_csv(
                        csv_path,
                        encoding=encoding,
                        parse_dates=['order date (DateOrders)', 'shipping date (DateOrders)'],
                        on_bad_lines='warn'
                    )
                    break
                except UnicodeDecodeError:
                    continue
            else:
                raise ValueError("Failed to decode CSV with common encodings")

            # Column renaming (same as your mapping)
            df = df.rename(columns={
            'Type': 'type',
            'Days for shipping (real)': 'days_for_shipping_real',
            'Days for shipment (scheduled)': 'days_for_shipment_scheduled',
            'Benefit per order': 'benefit_per_order',
            'Sales per customer': 'sales_per_customer',
            'Delivery Status': 'delivery_status',
            'Late_delivery_risk': 'late_delivery_risk',
            'Category Id': 'category_id',
            'Category Name': 'category_name',
            'Customer City': 'customer_city',
            'Customer Country': 'customer_country',
            'Customer Email': 'customer_email',
            'Customer Fname': 'customer_fname',
            'Customer Id': 'customer_id',
            'Customer Lname': 'customer_lname',
            'Customer Password': 'customer_password',
            'Customer Segment': 'customer_segment',
            'Customer State': 'customer_state',
            'Customer Street': 'customer_street',
            'Customer Zipcode': 'customer_zipcode',
            'Department Id': 'department_id',
            'Department Name': 'department_name',
            'Latitude': 'latitude',
            'Longitude': 'longitude',
            'Market': 'market',
            'Order City': 'order_city',
            'Order Country': 'order_country',
            'Order Customer Id': 'order_customer_id',
            'order date (DateOrders)': 'order_date',
            'Order Id': 'order_id',
            'Order Item Cardprod Id': 'order_item_cardprod_id',
            'Order Item Discount': 'order_item_discount',
            'Order Item Discount Rate': 'order_item_discount_rate',
            'Order Item Id': 'order_item_id',
            'Order Item Product Price': 'order_item_product_price',
            'Order Item Profit Ratio': 'order_item_profit_ratio',
            'Order Item Quantity': 'order_item_quantity',
            'Sales': 'sales',
            'Order Item Total': 'order_item_total',
            'Order Profit Per Order': 'order_profit_per_order',
            'Order Region': 'order_region',
            'Order State': 'order_state',
            'Order Status': 'order_status',
            'Order Zipcode': 'order_zipcode',
            'Product Card Id': 'product_card_id',
            'Product Category Id': 'product_category_id',
            'Product Description': 'product_description',
            'Product Image': 'product_image',
            'Product Name': 'product_name',
            'Product Price': 'product_price',
            'Product Status': 'product_status',
            'shipping date (DateOrders)': 'shipping_date',
            'Shipping Mode': 'shipping_mode'
            })

            # Batch insert
            session = self.SessionLocal()
            try:
                batch_size = 10_000
                records = df.to_dict('records')
                
                for i in range(0, len(records), batch_size):
                    session.bulk_insert_mappings(
                        SupplyChainOrder,
                        records[i:i + batch_size]
                    )
                    session.commit()
                    print(f"Imported batch {i//batch_size + 1}/{(len(records)//batch_size)+1}")
                    
            except Exception as e:
                session.rollback()
                raise ValueError(f"Database import failed: {str(e)}")
            finally:
                session.close()

        except Exception as e:
            raise ValueError(f"CSV processing failed: {str(e)}")

    def get_session(self):
        """Get a new database session"""
        return self.SessionLocal()