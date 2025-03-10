import streamlit as st
import pandas as pd
import sqlite3
from pathlib import Path
import time

# Must be the first Streamlit command
st.set_page_config(page_title="Hydrogen Tracker", layout="wide")

# Define the data directory path
DATA_DIR = Path("data")

# Custom CSS for additional styling (center title)
st.markdown("""
    <style>
        .center-title {
            text-align: center;
        }
    </style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=3600)
def load_and_process_data():
    """Load and process data with caching"""
    try:
        parquet_path = DATA_DIR / "projects.parquet"
        if parquet_path.exists():
            df = pd.read_parquet(parquet_path)
        else:
            with sqlite3.connect(DATA_DIR / "projects.db") as conn:
                # Create indexes using ORIGINAL DATABASE COLUMN NAMES
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS 
                    idx_project_name ON projects("DATABASE_Project name")
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS 
                    idx_country ON projects("DATABASE_Country")
                """)
                
                # Then load data
                df = pd.read_sql("SELECT * FROM projects", conn)

        # Column mapping remains the same
        column_mapping = {
            "Country": "country",
            "COUNTRY": "country",
            "DATABASE_Country": "country",
            "Capex (EUR)": "investment_eur",
            "Investment EUR": "investment_eur",
            "Investment": "investment_eur",
            "DATABASE_Announced Size": "investment_eur",
            "DATABASE_Project name": "project_name",
            "DATABASE_Status": "status",
            "DATABASE_Date online": "date_online",
            "DATABASE_Decomission date": "decommission_date",
            "DATABASE_Technology": "technology"
        }
        
        return df.rename(columns=column_mapping).infer_objects()
    
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()

def main():
    # Initialize session state for measuring load time
    if 'start_time' not in st.session_state:
        st.session_state.start_time = time.time()
    
    # Load data
    with st.spinner('Loading data...'):
        df = load_and_process_data()
    
    if df.empty:
        st.error("No data loaded - check your data sources")
        return

    # Convert date_online to datetime (if available) for sorting purposes
    if 'date_online' in df.columns:
        df['date_online'] = pd.to_datetime(df['date_online'], errors='coerce')

    # Centered Title
    st.markdown("<h1 class='center-title'>Hydrogen Tracker</h1>", unsafe_allow_html=True)

    # Search bar on main page (searches both project names and countries)
    search_input = st.text_input("Search for project names or countries", key="search")

    # Filter DataFrame based on search input
    if search_input:
        mask = (
            df['project_name'].str.contains(search_input, case=False, na=False) |
            df['country'].str.contains(search_input, case=False, na=False)
        )
        filtered_df = df[mask]
    else:
        filtered_df = df.copy()

    # Sort by date_online descending and take the first 10 projects
    if 'date_online' in filtered_df.columns:
        filtered_df = filtered_df.sort_values(by='date_online', ascending=False)
    top_projects = filtered_df.head(10)

    st.subheader("Recent Projects")
    # Display a simple table with key columns
    if not top_projects.empty:
        display_df = top_projects[['project_name', 'country', 'date_online', 'status']].copy()
        if 'date_online' in display_df.columns:
            display_df['date_online'] = display_df['date_online'].dt.date  # show only the date part
        st.table(display_df)
    else:
        st.info("No projects found matching your search.")

    st.write("Page generated in:", round(time.time() - st.session_state.start_time, 2), "seconds")

if __name__ == "__main__":
    main()
