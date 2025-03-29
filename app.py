import streamlit as st
import pandas as pd
from src.agents.supply_chain_agent import SupplyChainAgent

# Page configuration
st.set_page_config(
    page_title="Supply Chain Query Assistant",
    page_icon="🚚",
    layout="wide"
)

# Initialize session state for pagination
if 'offset' not in st.session_state:
    st.session_state.offset = 0
if 'limit' not in st.session_state:
    st.session_state.limit = 10
if 'last_query' not in st.session_state:
    st.session_state.last_query = ""

# Header
st.title("Supply Chain Query Assistant")
st.markdown("Ask questions about your supply chain data in natural language")

# Initialize agent (use st.cache_resource to prevent reinitialization on rerun)
@st.cache_resource
def get_agent():
    return SupplyChainAgent()

agent = get_agent()

# Query input
query = st.text_input("Enter your question:", 
                     placeholder="e.g., Show 5 orders from Puerto Rico")

# Pagination controls
col1, col2 = st.columns([1, 4])
with col1:
    st.session_state.limit = st.number_input("Results per page:", 
                                           min_value=5, max_value=50, value=st.session_state.limit)

# Process query when submitted
if query:
    # Reset pagination when new query is submitted
    if query != st.session_state.last_query:
        st.session_state.offset = 0
        st.session_state.last_query = query
    
    # Execute query with pagination
    response = agent.handle_query(
        query, 
        limit=st.session_state.limit, 
        offset=st.session_state.offset
    )
    
    # Display the SQL query used (in expandable section for developers)
    with st.expander("SQL Query Used"):
        st.code(response.get('query_used', 'No SQL query available'), language="sql")
    
    # Display results
    st.subheader("Results")
    
    if response['status'] == 'success':
        # Show textual response
        st.markdown(response['response'])
        
        # If results exist, display as table too
        if 'results' in response and len(response.get('results', [])) > 0:
            st.dataframe(pd.DataFrame(response.get('results', [])))
            
            # Pagination navigation
            pagination = response.get('pagination', {})
            total = pagination.get('total', 0)
            
            col1, col2, col3 = st.columns([1, 2, 1])
            
            with col1:
                if st.session_state.offset > 0:
                    if st.button("⬅️ Previous Page"):
                        st.session_state.offset = max(0, st.session_state.offset - st.session_state.limit)
                        st.rerun()
            
            with col2:
                st.markdown(f"Showing {st.session_state.offset + 1} to {min(st.session_state.offset + st.session_state.limit, total)} of {total} results")
            
            with col3:
                if pagination.get('has_more', False):
                    if st.button("Next Page ➡️"):
                        st.session_state.offset += st.session_state.limit
                        st.rerun()
    
    elif response['status'] == 'invalid':
        st.warning(response['response'])
    
    else:
        st.error(response['response'])

# Footer with instructions
with st.expander("Usage Tips"):
    st.markdown("""
    - Ask questions in plain English about supply chain data
    - You can ask about orders, deliveries, products, inventory, etc.
    - Try being specific about what information you need
    - Examples:
        - "Show me late deliveries from last month"
        - "List high-risk suppliers with pending orders"
        - "What customers have the highest order values?"
    """)

# Clean up on page close
def cleanup():
    agent.close()

# Register cleanup function
import atexit
atexit.register(cleanup)