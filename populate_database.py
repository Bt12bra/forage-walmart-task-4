import pandas as pd
import sqlite3
import os
import glob

# Configuration
repo_dir = "forage-walmart-task-4"  # Directory containing spreadsheets and database
db_name = f"{repo_dir}/shipping_data.db"
table_name = "shipments"

# Database schema
create_table_sql = """
CREATE TABLE IF NOT EXISTS shipments (
    shipment_identifier TEXT PRIMARY KEY,
    product TEXT,
    quantity INTEGER,
    origin TEXT,
    destination TEXT
)
"""

def read_spreadsheet(file_path):
    """Read an Excel or CSV spreadsheet into a pandas DataFrame."""
    try:
        if file_path.endswith((".xlsx", ".xls")):
            df = pd.read_excel(file_path)
        elif file_path.endswith(".csv"):
            df = pd.read_csv(file_path)
        else:
            raise ValueError(f"Unsupported file format: {file_path}")
        print(f"Columns in {file_path}: {list(df.columns)}")
        return df
    except Exception as e:
        print(f"Error reading {file_path}: {str(e)}")
        return None

def process_spreadsheet_0(df):
    """Process Spreadsheet 0: directly map to database schema."""
    try:
        required_columns = ["shipment_identifier", "product", "quantity", "origin", "destination"]
        available_columns = [col for col in required_columns if col in df.columns]
        if not all(col in df.columns for col in ["shipment_identifier", "product", "quantity"]):
            print(f"Warning: Missing required columns in Spreadsheet 0. Available: {df.columns}")
            return None
        df = df[available_columns]
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)
        df = df.drop_duplicates(subset=["shipment_identifier"])
        return df
    except Exception as e:
        print(f"Error processing Spreadsheet 0: {str(e)}")
        return None

def process_spreadsheets_1_and_2(df1, df2):
    """Process Spreadsheets 1 and 2: combine by shipment_identifier and aggregate quantities."""
    try:
        if not all(col in df1.columns for col in ["shipment_identifier", "product", "quantity"]):
            print(f"Warning: Missing required columns in Spreadsheet 1. Available: {df1.columns}")
            return None
        if not all(col in df2.columns for col in ["shipment_identifier", "origin", "destination"]):
            print(f"Warning: Missing required columns in Spreadsheet 2. Available: {df2.columns}")
            return None
        df1_grouped = df1.groupby(["shipment_identifier", "product"])["quantity"].sum().reset_index()
        df_merged = pd.merge(
            df1_grouped,
            df2[["shipment_identifier", "origin", "destination"]],
            on="shipment_identifier",
            how="left"
        )
        df_merged["origin"].fillna("Unknown", inplace=True)
        df_merged["destination"].fillna("Unknown", inplace=True)
        df_merged["quantity"] = pd.to_numeric(df_merged["quantity"], errors="coerce").fillna(0).astype(int)
        df_merged = df_merged.drop_duplicates(subset=["shipment_identifier"])
        return df_merged
    except Exception as e:
        print(f"Error processing Spreadsheets 1 and 2: {str(e)}")
        return None

def insert_into_database(df, db_name, table_name):
    """Insert data into SQLite database."""
    if df is None or df.empty:
        print("No data to insert into database.")
        return
    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        df.to_sql(table_name, conn, if_exists="append", index=False)
        conn.commit()
        print(f"Inserted {len(df)} records into {table_name}.")
    except Exception as e:
        print(f"Database error: {str(e)}")
    finally:
        conn.close()

def main():
    try:
        # Verify current directory
        print(f"Current working directory: {os.getcwd()}")
        if not os.path.exists(repo_dir):
            print(f"Error: Directory {repo_dir} does not exist.")
            return
        
        # Find spreadsheet files dynamically
        spreadsheet_files = glob.glob(f"{repo_dir}/*.xlsx") + glob.glob(f"{repo_dir}/*.csv")
        print(f"Found files: {spreadsheet_files}")
        if not spreadsheet_files:
            print(f"Error: No .xlsx or .csv files found in {repo_dir}")
            return
        
        # Expected files
        spreadsheet_0 = None
        spreadsheet_1 = None
        spreadsheet_2 = None
        
        for file in spreadsheet_files:
            file_lower = file.lower()
            if any(x in file_lower for x in ["spreadsheet0", "sheet0"]):
                spreadsheet_0 = file
            elif any(x in file_lower for x in ["spreadsheet1", "sheet1"]):
                spreadsheet_1 = file
            elif any(x in file_lower for x in ["spreadsheet2", "sheet2"]):
                spreadsheet_2 = file
        
        # Process Spreadsheet 0
        if spreadsheet_0:
            print(f"Processing Spreadsheet 0: {spreadsheet_0}")
            df0 = read_spreadsheet(spreadsheet_0)
            if df0 is not None:
                df0_formatted = process_spreadsheet_0(df0)
                insert_into_database(df0_formatted, db_name, table_name)
            else:
                print("Failed to process Spreadsheet 0.")
        else:
            print("Spreadsheet 0 not found.")
        
        # Process Spreadsheets 1 and 2
        if spreadsheet_1 and spreadsheet_2:
            print(f"Processing Spreadsheets 1 and 2: {spreadsheet_1}, {spreadsheet_2}")
            df1 = read_spreadsheet(spreadsheet_1)
            df2 = read_spreadsheet(spreadsheet_2)
            if df1 is not None and df2 is not None:
                df12_formatted = process_spreadsheets_1_and_2(df1, df2)
                insert_into_database(df12_formatted, db_name, table_name)
            else:
                print("Failed to process Spreadsheets 1 or 2.")
        else:
            print(f"Spreadsheet 1 or 2 not found. Found: {spreadsheet_files}")
        
        # Verify database
        try:
            conn = sqlite3.connect(db_name)
            query = f"SELECT * FROM {table_name} LIMIT 5"
            result = pd.read_sql(query, conn)
            print("Sample data from database:")
            print(result)
            conn.close()
        except Exception as e:
            print(f"Database verification error: {str(e)}")
        
    except Exception as e:
        print(f"Main error: {str(e)}")

if __name__ == "__main__":
    main()
