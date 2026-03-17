import pandas as pd
import sqlite3
import ast
from pathlib import Path

# --- 1. THE MATH ENGINE ---
def calculate_bps(timestamp_str):
    """
    Takes a string of timestamps from Scoutradioz (e.g., "[1.2, 2.5, 3.1]")
    and calculates the average Balls Per Second (BPS).
    """
    # If the cell is empty, NaN, or completely blank, return 0.0
    if pd.isna(timestamp_str) or str(timestamp_str).strip() == "":
        return 0.0
    
    try:
        # Safely evaluate the string into a Python list
        timestamps = ast.literal_eval(str(timestamp_str))
        
        # We need at least 2 shots to calculate a time interval
        if not isinstance(timestamps, list) or len(timestamps) < 2:
            return 0.0
        
        # Sort just in case they are out of order
        timestamps.sort()
        
        # Math: (Total Balls - 1) / (Time of Last Shot - Time of First Shot)
        total_shots = len(timestamps)
        time_duration = timestamps[-1] - timestamps[0]
        
        if time_duration == 0:
            return 0.0
            
        bps = (total_shots - 1) / time_duration
        return round(bps, 2)
        
    except (ValueError, SyntaxError):
        # If the scouter corrupted the data string, fail safely instead of crashing
        return 0.0

# --- 2. THE PIPELINE ---
def process_match_data(csv_path, db_path):
    """Reads the raw CSV, transforms the data, and loads it into SQLite."""
    print(f" Processing file: {csv_path}...")
    
    # 1. Extract: Read the CSV
    try:
        df = pd.read_csv(csv_path)
    except FileNotFoundError:
        print(f"Error: Could not find {csv_path}. Please check the path.")
        return

    # 2. Transform: Calculate BPS
    # Scoutradioz usually appends "_timestamps" to counter variables
    # We will look for them, calculate BPS, and map them to the database columns
    if 'autoFuel_timestamps' in df.columns:
        df['auto_bps'] = df['autoFuel_timestamps'].apply(calculate_bps)
    else:
        df['auto_bps'] = 0.0

    if 'teleFuel_timestamps' in df.columns:
        df['tele_bps'] = df['teleFuel_timestamps'].apply(calculate_bps)
    else:
        df['tele_bps'] = 0.0

    # Fill any missing numeric values with 0 and text with 'N/A' to prevent SQL errors
    num_cols = df.select_dtypes(include=['float64', 'int64']).columns
    df[num_cols] = df[num_cols].fillna(0)
    df = df.fillna('N/A')

    # 3. Load: Push to SQLite using UPSERT logic
    conn = sqlite3.connect(db_path)
    cursor = None  # FIX: initialize before try so finally block is always safe

    # Use SQLite's INSERT OR REPLACE to move data from temp to the real table
    # This prevents your composite Primary Key from breaking if you run the script twice
    try:
        # FIX: moved inside try so if to_sql fails, cursor is still safely guarded in finally
        df.to_sql('temp_match_data', conn, if_exists='replace', index=False)

        # Get matching columns between the dataframe and our DB schema
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(match_data)")
        db_columns = [row[1] for row in cursor.fetchall()]
        
        common_cols = [c for c in df.columns if c in db_columns]
        cols_string = ", ".join(common_cols)
        
        upsert_query = f"""
        INSERT OR REPLACE INTO match_data ({cols_string})
        SELECT {cols_string} FROM temp_match_data;
        """
        cursor.execute(upsert_query)
        conn.commit()
        
        print(f"Successfully processed and loaded {len(df)} rows into 'match_data'!")
        
    except sqlite3.Error as e:
        print(f"Database error during UPSERT: {e}")
    finally:
        # Clean up the temporary table
        if cursor is not None:  # FIX: guard so finally doesn't crash if to_sql failed
            cursor.execute("DROP TABLE IF EXISTS temp_match_data")
        conn.close()

if __name__ == "__main__":
    #project_root = Path(__file__).parent.parent
    #project_root = Path(r"C:\Users\regg0\Desktop\FRC Strat Folder")
    project_root = Path(__file__).resolve().parent
    csv_file = project_root / 'data' / 'match_export.csv'
    database_file = project_root / 'database' / 'scouting_2026.db'

    (project_root / 'data').mkdir(parents=True, exist_ok=True)
    (project_root / 'database').mkdir(parents=True, exist_ok=True)
    
    # Run the engine
    process_match_data(csv_file, database_file)
