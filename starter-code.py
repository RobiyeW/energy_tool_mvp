# Let’s start by loading the uploaded files and exploring their structure! I’ll check the first few rows to understand the data and decide the next steps for cleaning, structuring, and manipulation.
import pandas as pd

# Load the uploaded Excel files
infrastructure_df = pd.ExcelFile("/mnt/data/IEA Hydrogen Infrastracture Database.xlsx")
production_df = pd.ExcelFile("/mnt/data/IEA Hydrogen Production Projects Database.xlsx")

# Check the sheet names to understand the data organization
infrastructure_df.sheet_names, production_df.sheet_names