import streamlit as st
import pandas as pd
import sqlite3
from pathlib import Path
import textwrap
from typing import Optional

# Configuration
DATA_DIR = Path("data")
ITEMS_PER_PAGE = 9  # 3 columns × 3 rows

# Must be the first Streamlit command
st.set_page_config(page_title="Hydrogen Tracker", layout="wide")

# Initialize session state variables if not already set
if "show_filters" not in st.session_state:
    st.session_state.show_filters = False
if "page" not in st.session_state:
    st.session_state.page = 1

# Custom CSS for styling
st.markdown("""
    <style>
        .centered-title {
            text-align: center;
            margin-top: 2rem;
            margin-bottom: 1rem;
        }
        .search-container {
            max-width: 600px;
            margin: 0 auto 2rem auto;
        }
        .card-grid {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 1.5rem;
            padding: 1rem 0;
        }
        .card {
            border: 1px solid #e0e0e0;
            border-radius: 8px;
            padding: 1.2rem;
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
            background: white;
            transition: transform 0.2s;
            height: 100%;
        }
        .card:hover {
            transform: translateY(-3px);
        }
        .card h3 {
            color: #1a73e8;
            margin: 0 0 0.8rem 0;
            font-size: 1.1rem;
        }
        .card p {
            margin: 0.3rem 0;
            font-size: 0.9rem;
            color: #5f6368;
        }
        .investment {
            color: #0d652d;
            font-weight: 500;
        }
        .pagination-row {
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 2rem;
            margin-bottom: 1rem;
        }
        .hamburger-btn {
            font-size: 1.5rem;
            background: none;
            border: none;
            cursor: pointer;
        }
        button {
            cursor: pointer !important;
            user-select: none;
        }
    </style>
""", unsafe_allow_html=True)


@st.cache_data(ttl=3600)
def load_and_process_data() -> Optional[pd.DataFrame]:
    """Load and process data with caching and error handling."""
    try:
        parquet_path = DATA_DIR / "projects.parquet"
        if parquet_path.exists():
            df = pd.read_parquet(parquet_path)
        else:
            with sqlite3.connect(DATA_DIR / "projects.db") as conn:
                conn.execute("CREATE INDEX IF NOT EXISTS idx_project_name ON projects('DATABASE_Project name')")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_country ON projects('DATABASE_Country')")
                df = pd.read_sql("SELECT * FROM projects", conn)

        column_mapping = {
            "DATABASE_Country": "country",
            "DATABASE_Announced Size": "investment_eur",
            "DATABASE_Project name": "project_name",
            "DATABASE_Status": "status",
            "DATABASE_Date online": "date_online",
            "DATABASE_Technology": "technology"
        }
        
        df = df.rename(columns=column_mapping)
        df['date_online'] = pd.to_datetime(df['date_online'], errors='coerce')
        
        if 'investment_eur' in df.columns:
            df['investment_eur'] = pd.to_numeric(df['investment_eur'], errors='coerce').fillna(0)
        
        for col in ['project_name', 'country', 'status', 'technology']:
            if col in df.columns:
                df[col] = df[col].fillna("N/A")
                
        return df
    
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return None

def generate_card(project: pd.Series) -> str:
    """Generate HTML card for a project."""
    try:
        date_online_str = project['date_online'].strftime('%Y-%m') if pd.notnull(project['date_online']) else 'N/A'
        card_html = textwrap.dedent(f"""\
            <div class="card">
                <h3>{project['project_name']}</h3>
                <p><strong>Country:</strong> {project['country']}</p>
                <p><strong>Status:</strong> {project['status']}</p>
                <p><strong>Technology:</strong> {project['technology']}</p>
                <p class="investment">Investment: €{project['investment_eur']:,.0f}</p>
                <p><strong>Date Online:</strong> {date_online_str}</p>
            </div>""")
        return card_html
    except KeyError as e:
        st.error(f"Missing data field: {e}")
        return ""

def apply_filters(df: pd.DataFrame, search_query: str, filters: dict = None) -> pd.DataFrame:
    """Apply search and additional filters to the dataframe."""
    if search_query:
        df = df[
            df['project_name'].str.contains(search_query, case=False, na=False) |
            df['country'].str.contains(search_query, case=False, na=False)
        ]
    if filters:
        for field, values in filters.items():
            if values:
                df = df[df[field].isin(values)]
    return df

def sort_data(df: pd.DataFrame) -> pd.DataFrame:
    """Sort projects by newest date."""
    return df.sort_values("date_online", ascending=False)

def next_page():
    """Go to the next page."""
    st.session_state.page += 1

def previous_page():
    """Go to the previous page."""
    st.session_state.page -= 1

def main():
    st.markdown("<h1 class='centered-title'>Hydrogen Projects Tracker</h1>", unsafe_allow_html=True)
    
    # Search bar
    search_query = st.text_input("Search projects by name or country", key="search")
    
    # Hamburger menu for filters
    if st.button("☰ Filters", key="hamburger", help="Toggle filter controls"):
        st.session_state.show_filters = not st.session_state.show_filters
    
    # Load data
    df = load_and_process_data()
    if df is None:
        return

    # Show filters if toggled
    filters = {}
    if st.session_state.show_filters:
        with st.container():
            st.subheader("Filter Options")
            col1, col2, col3 = st.columns(3)
            filters['status'] = col1.multiselect("Status", df['status'].unique())
            filters['technology'] = col2.multiselect("Technology", df['technology'].unique())
            filters['country'] = col3.multiselect("Country", df['country'].unique())

    # Apply search and filters
    filtered_df = apply_filters(df, search_query, filters)
    sorted_df = sort_data(filtered_df)

    total_items = len(sorted_df)
    total_pages = max((total_items - 1) // ITEMS_PER_PAGE + 1, 1)

    # Pagination
    col_prev, col_page, col_next = st.columns([1, 2, 1])
    with col_prev:
        st.button("◀ Previous", disabled=(st.session_state.page <= 1), on_click=previous_page)
    with col_page:
        st.markdown(f"<div style='text-align: center;'>Page {st.session_state.page} of {total_pages}</div>", unsafe_allow_html=True)
    with col_next:
        st.button("Next ▶", disabled=(st.session_state.page >= total_pages), on_click=next_page)

    # Show project cards
    start_idx = (st.session_state.page - 1) * ITEMS_PER_PAGE
    end_idx = start_idx + ITEMS_PER_PAGE
    paged_df = sorted_df.iloc[start_idx:end_idx]

    if not paged_df.empty:
        cards_html = "<div class='card-grid'>" + "".join([generate_card(project) for _, project in paged_df.iterrows()]) + "</div>"
        st.markdown(cards_html, unsafe_allow_html=True)
    else:
        st.info("No projects found.")

    st.markdown(f"**Total projects found:** {total_items}")
    # Print a sample of date_online to inspect the format
    st.write(df['date_online'].dropna().head(10))


if __name__ == "__main__":
    main()
