import sqlite3
import pandas as pd
import numpy as np
import re
from pathlib import Path
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def setup_data_directory():
    """
    Create and return the raw and processed directories.
    """
    raw_dir = Path("data/raw")
    processed_dir = Path("data/processed")
    raw_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(exist_ok=True)
    logging.info("Setting up directories completed")
    return raw_dir, processed_dir

def clean_column_name(name):
    """
    Clean column names for Parquet compatibility by replacing whitespace and newlines.
    """
    return re.sub(r'\s+', ' ', name).replace('\n', ' ').strip()

def clean_numeric(value):
    """
    Convert values to float after removing non-numeric characters.
    """
    try:
        # Remove non-digit and non-period characters before conversion.
        return float(re.sub(r'[^\d.]', '', str(value)))
    except Exception as e:
        logging.warning(f"Could not convert value {value} to float: {e}")
        return np.nan

def excel_date_converter(date_val):
    """
    Convert Excel dates (numeric or string) to ISO date string format.
    """
    try:
        if isinstance(date_val, (int, float)):
            # Excel date conversion: Excel's epoch starts on 1899-12-30
            return (datetime(1899, 12, 30) + pd.DateOffset(days=date_val)).strftime('%Y-%m-%d')
        
        # Specify the date format (adjust based on your data)
        return pd.to_datetime(date_val, format='%Y-%m-%d', errors='coerce').strftime('%Y-%m-%d')
    
    except Exception as e:
        logging.warning(f"Could not convert date {date_val}: {e}")
        return None


def clean_iea_data(file_path):
    """
    Process IEA Excel data into a clean DataFrame and save as a Parquet file.
    Also prints the entire cleaned DataFrame.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"Data file not found: {file_path}")
    
    try:
        # Read the Excel file with multi-level header from the 'Projects' sheet.
        df = pd.read_excel(file_path, sheet_name='Projects', header=[0, 1])
    except Exception as e:
        logging.error(f"Error reading Excel file: {e}")
        raise

    # Process column names for clarity and Parquet compatibility.
    df.columns = [clean_column_name('_'.join(col).strip()) for col in df.columns]
    df = df.loc[:, ~df.columns.str.contains('Unnamed')]
    
    logging.info(f"Processed columns: {df.columns.tolist()}")

    # Ensure that the project IDs are strings and drop rows missing an ID.
    df['DATABASE_Ref'] = df['DATABASE_Ref'].astype(str)
    df = df.dropna(subset=['DATABASE_Ref'])
    
    # Clean numeric columns based on keywords in the column names.
    numeric_cols = [col for col in df.columns if 'capacity' in col.lower() or 'size' in col.lower()]
    for col in numeric_cols:
        df[col] = df[col].apply(clean_numeric).astype('float32')
    
    # Convert date columns to ISO formatted strings.
    date_cols = [col for col in df.columns if 'date' in col.lower()]
    for col in date_cols:
        df[col] = df[col].apply(excel_date_converter)
    
    # Remove duplicate project IDs, keeping the first occurrence.
    if df['DATABASE_Ref'].duplicated().any():
        logging.info("Duplicate IDs found - keeping first occurrence")
        df = df.drop_duplicates('DATABASE_Ref', keep='first')
    
    # Ensure all object type columns are strings.
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str)

    # Set pandas display options to show all rows and columns.
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    
    # Print the entire cleaned DataFrame.
    logging.info("Cleaned DataFrame (full view):")
    logging.info("\n" + df.to_string())

    # Save the cleaned DataFrame to a Parquet file.
    clean_path = Path("data/processed/cleaned.parquet")
    try:
        df.to_parquet(clean_path, engine='pyarrow')
        logging.info(f"Cleaned data saved to {clean_path}")
    except Exception as e:
        logging.error(f"Error saving Parquet file: {e}")
        raise

    return df, clean_path

def create_sqlite_mirror(parquet_path):
    """
    Create a SQLite database mirror from the cleaned Parquet data.
    """
    try:
        df = pd.read_parquet(parquet_path)
    except Exception as e:
        logging.error(f"Error reading Parquet file: {e}")
        raise

    try:
        with sqlite3.connect('data/projects.db') as conn:
            df.to_sql('projects', conn, if_exists='replace', index=False)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ref ON projects([DATABASE_Ref])")
        logging.info("SQLite database created and index set on DATABASE_Ref")
    except Exception as e:
        logging.error(f"Error creating SQLite database: {e}")
        raise

def print_dataframe_table(df):
    """
    Print the DataFrame as a formatted table.
    """
    try:
        print("\nFinal Cleaned DataFrame:")
        print(df.to_markdown(tablefmt="grid"))
    except Exception as e:
        logging.error(f"Error printing DataFrame: {e}")
        raise

def export_dataframe_to_excel(df, output_path):
    """
    Export the DataFrame to an Excel file.
    """
    try:
        df.to_excel(output_path, index=False)
        logging.info(f"Data exported to Excel at {output_path}")
    except Exception as e:
        logging.error(f"Error exporting DataFrame to Excel: {e}")
        raise

if __name__ == "__main__":
    raw_dir, _ = setup_data_directory()
    data_file = raw_dir / "IEA Hydrogen Production Projects Database.xlsx"
    
    try:
        # Clean the data and get the DataFrame and Parquet path.
        df, cleaned_parquet = clean_iea_data(data_file)
        # Create the SQLite mirror from the Parquet file.
        create_sqlite_mirror(cleaned_parquet)
        # Print the DataFrame as a table.
        print_dataframe_table(df)
        # Export the DataFrame to an Excel file.
        excel_output = Path("data/processed/cleaned.xlsx")
        export_dataframe_to_excel(df, excel_output)
        logging.info("Pipeline completed successfully!")
    except Exception as e:
        logging.error(f"Final error: {str(e)}")
