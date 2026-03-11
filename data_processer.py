import sqlite3 as sq
import os
import ast
import pandas as pd

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
        # If corrupted data string, fail safely instead of crashing
        return 0.0

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
    # Scoutradioz usually appends "timestamps" to counter variables
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
    
    # Push the dataframe to a temporary table first
    df.to_sql('temp_match_data', conn, if_exists='replace', index=False)
