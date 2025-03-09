import sqlite3
import pandas as pd
import numpy as np
import re
from pathlib import Path
from datetime import datetime

def setup_data_directory():
    raw_dir = Path("data/raw")
    processed_dir = Path("data/processed")
    raw_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(exist_ok=True)
    print("Setting up directories completed")
    return raw_dir, processed_dir

def clean_column_name(name):
    """Clean column names for Parquet compatibility"""
    return re.sub(r'\s+', ' ', name).replace('\n', ' ').strip()

def clean_numeric(value):
    """Safe numeric conversion"""
    try:
        return float(re.sub(r'[^\d.]', '', str(value)))
    except:
        return np.nan

def excel_date_converter(date_val):
    """Convert Excel dates to ISO strings"""
    try:
        if isinstance(date_val, (int, float)):
            return (datetime(1899, 12, 30) + pd.DateOffset(days=date_val)).strftime('%Y-%m-%d')
        return pd.to_datetime(date_val).strftime('%Y-%m-%d')
    except:
        return None

def clean_iea_data(file_path):
    """Process IEA Excel data into clean Parquet format"""
    if not file_path.exists():
        raise FileNotFoundError(f"Data file not found: {file_path}")
    
    # Read and process headers
    df = pd.read_excel(file_path, sheet_name='Projects', header=[0, 1])
    df.columns = [clean_column_name('_'.join(col).strip()) for col in df.columns]
    df = df.loc[:, ~df.columns.str.contains('Unnamed')]
    
    print("Processed columns:", df.columns.tolist())
    
    # Clean project IDs
    df['DATABASE_Ref'] = df['DATABASE_Ref'].astype(str)
    df = df.dropna(subset=['DATABASE_Ref'])
    
    # Clean numeric columns
    numeric_cols = [col for col in df.columns if 'capacity' in col.lower() or 'size' in col.lower()]
    for col in numeric_cols:
        df[col] = df[col].apply(clean_numeric).astype('float32')
    
    # Clean dates
    date_cols = [col for col in df.columns if 'date' in col.lower()]
    for col in date_cols:
        df[col] = df[col].apply(excel_date_converter)
    
    # Handle duplicates
    if df['DATABASE_Ref'].duplicated().any():
        print("Duplicate IDs found - keeping first")
        df = df.drop_duplicates('DATABASE_Ref', keep='first')
    
    # Ensure string types
    for col in df.select_dtypes('object'):
        df[col] = df[col].astype(str)
    
    # Save to Parquet
    clean_path = Path("data/processed/cleaned.parquet")
    df.to_parquet(clean_path, engine='pyarrow')
    return clean_path

def create_sqlite_mirror(parquet_path):
    """Create SQLite database"""
    df = pd.read_parquet(parquet_path)
    with sqlite3.connect('data/projects.db') as conn:
        df.to_sql('projects', conn, if_exists='replace', index=False)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ref ON projects([DATABASE_Ref])")

if __name__ == "__main__":
    raw_dir, _ = setup_data_directory()
    data_file = raw_dir / "IEA Hydrogen Production Projects Database.xlsx"
    
    try:
        cleaned = clean_iea_data(data_file)
        create_sqlite_mirror(cleaned)
        print("Pipeline completed successfully!")
    except Exception as e:
        print(f"Final error: {str(e)}")