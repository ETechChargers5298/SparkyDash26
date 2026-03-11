import sqlite3 as sq
import os
import ast
import pandas as pd

def process_match_data(csv_path, db_path):
    print ("Processing file") 
    try:
        df = pd.read_csv(csv_path)
    except FileNotFoundError:
        print ("Error could not find file, please check the path")
        return
    
    
    
    
    if "auto_fuel_timestamps" in df.columns:
        df["auto_bps"] = df["auto_fuel_timestamps"].apply(caluclate_bps)
    
    else:
        df ["auto_bps"] = 0.0
        
    if "tele_fuel_timestamps" in df.columns:
        df["tele_bps"] = df["tele_fuel_timestamps"].apply(calculate_bps)
    
    else:
        df ["tele_bps"] = 0.0
    
    
    # no zeros
    num_columns = df.select_dtypes(include=["float64", "int64"]).columns
    df[num_columns] = df[num_columns].fillna(0)
    df = df.fillna("N/A")
    
    # verification of scout data
    conn = sq.connect(db_path)
    df.to_sql("temporary_match_data", conn, if_exists="replace", index=False)
    
    def calculate_bps(timestamp_List):
        if pd.isna(timestamp_List) or str(timestamp_List).strip() == "":
            return 0.0
        try:
            timestamp = ast.literal_eval(str(timestamp_List))
            
            if not isinstance(timestamp, list) or len(timestamp) < 2:
                return 0.0
            
            timestamp.sort()
            total_shots = len(timestamp)
            time_duration = timestamp[-1]-timestamp[0]