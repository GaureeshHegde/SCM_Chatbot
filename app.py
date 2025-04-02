# app.py (root directory)
import streamlit as st
from src.agents.supply_chain_agent import SupplyChainAgent
import sys
from pathlib import Path

# Configure Python path
sys.path.insert(0, str(Path(__file__).parent))

# Page Config
st.set_page_config(
    page_title="Supply Chain Query Assistant",
    page_icon="🚛",
    layout="wide"
)

# Initialize agent with caching
@st.cache_resource
def get_agent():
    return SupplyChainAgent()

def main():
    st.title("🔍 Supply Chain Query Assistant")
    st.markdown("Ask natural language questions about your supply chain data")
    
    # Initialize session state
    if 'page' not in st.session_state:
        st.session_state.page = 1
    
    agent = get_agent()
    
    # Query Input
    query = st.text_area(
        "Enter your question:", 
        placeholder="e.g., 'Show late deliveries from Puerto Rico'",
        height=100
    )
    
    # Pagination controls
    col1, col2 = st.columns(2)
    with col1:
        limit = st.number_input("Results per page", min_value=1, max_value=50, value=10)
    
    # Process Query
    if st.button("Submit Query", type="primary"):
        if not query.strip():
            st.warning("Please enter a question")
            return
            
        with st.spinner("Analyzing your query..."):
            try:
                offset = (st.session_state.page - 1) * limit
                response = agent.handle_query(query, limit=limit, offset=offset)
                
                # Display Results
                st.subheader("Results")
                st.markdown(response["response"])
                
                # Raw Data
                with st.expander("View Raw Data"):
                    if response['status'] == 'success':
                        st.json(response.get('results', []))
                
                # Technical Details
                with st.expander("Technical Details"):
                    st.code(response.get('query_used', ''), language="sql")
                    st.write("Pagination:", response.get('pagination', {}))
                
                # Pagination Controls
                pagination = response.get('pagination', {})
                if pagination.get('has_more', False) or st.session_state.page > 1:
                    col1, col2, _ = st.columns([1, 1, 3])
                    if col1.button("⬅️ Previous") and st.session_state.page > 1:
                        st.session_state.page -= 1
                        st.rerun()
                    if col2.button("Next ➡️") and pagination.get('has_more', False):
                        st.session_state.page += 1
                        st.rerun()
            
            except Exception as e:
                st.error(f"Error: {str(e)}")

    # Usage Tips
    with st.expander("💡 Query Examples"):
        st.markdown("""
        - "Show orders with late deliveries"
        - "List top 10 products by sales"
        - "Which suppliers have pending shipments?"
        - "Find orders from California last month"
        """)

if __name__ == "__main__":
    main()