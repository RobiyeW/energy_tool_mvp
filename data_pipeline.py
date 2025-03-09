import sqlite3
import duckdb
import pandas as pd
from pathlib import Path

def setup_data_directory():
    raw_dir = Path("data/raw")
    processed_dir = Path("data/processed")
    raw_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(exist_ok=True)
    print("Setting up the directories completed")
    return raw_dir, processed_dir

def clean_iea_data(file_path):
    """Process IEA Excel data into clean Parquet format"""
    if not file_path.exists():
        raise FileNotFoundError(f"Data file not found: {file_path}")
    
    df = pd.read_excel(file_path, sheet_name='Projects')
    
    # Print out the column names for debugging
    print("Available columns in the dataset:", df.columns.tolist())
    
    # Find the first non-unnamed column
    valid_columns = [col for col in df.columns if not col.startswith('Unnamed')]
    if not valid_columns:
        raise KeyError("No valid column names found. Check the Excel file structure.")
    
    project_id_column = valid_columns[0]
    print(f"Using '{project_id_column}' as the project ID column.")
    
    # Essential cleaning
    df = (
        df.dropna(subset=[project_id_column])
          .rename(columns={project_id_column: 'project_id'})
          .rename(columns=str.lower)
          .astype({'investment': 'float32'})
          .convert_dtypes()
    )
    
    # Handle datetime conversion safely
    df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')
    
    # Check for duplicate project IDs
    if df['project_id'].duplicated().any():
        print("Warning: Duplicate project IDs found!")
    
    # Save to compressed Parquet
    clean_path = Path("data/processed/cleaned.parquet")
    df.to_parquet(clean_path, compression='snappy')
    return clean_path

def create_sqlite_mirror(parquet_path):
    """Create SQLite mirror for Streamlit access"""
    df = pd.read_parquet(parquet_path)
    with sqlite3.connect('data/projects.db') as conn:
        df.to_sql('projects', conn, if_exists='replace', index=False)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_country ON projects(country)")

if __name__ == "__main__":
    raw_dir, _ = setup_data_directory()
    sample_data = raw_dir / "IEA Hydrogen Production Projects Database.xlsx"
    
    # Demo flow
    cleaned = clean_iea_data(sample_data)
    create_sqlite_mirror(cleaned)
    print("Data pipeline executed successfully!")
