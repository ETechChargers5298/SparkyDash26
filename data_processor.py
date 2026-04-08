import pandas as pd
import sqlite3
from pathlib import Path

# --- THE PIPELINE ---
def process_match_data(csv_path, db_path):
    """Reads the raw CSV, transforms the data, and loads it into SQLite."""
    print(f" Processing file: {csv_path}...")
    
    # 1. Extract: Read the CSV
    try:
        df = pd.read_csv(csv_path)
    except FileNotFoundError:
        print(f"Error: Could not find {csv_path}. Please check the path.")
        return

    # 2. Transform: Normalize Scoutradioz column names
    # Scoutradioz exports team_key as "frcXXXX" — strip prefix and rename to teamNumber
    if 'team_key' in df.columns and 'teamNumber' not in df.columns:
        df['teamNumber'] = df['team_key'].str.replace('frc', '', regex=False).astype(int)

    # Scoutradioz exports match_number — rename to matchNumber if needed
    if 'match_number' in df.columns and 'matchNumber' not in df.columns:
        df = df.rename(columns={'match_number': 'matchNumber'})

    # 3. Transform: Proportional Score
    # Tier weights: Elite=4, High=3, Medium=2, Low=1, None=0
    TIER_WEIGHTS = {'Elite': 4, 'High': 3, 'Medium': 2, 'Low': 1, 'None': 0}

    if 'robotTier' not in df.columns:
        df['robotTier'] = 'None'
    df['robotTier'] = df['robotTier'].fillna('None').astype(str).str.strip()
    df['tier_weight'] = df['robotTier'].map(TIER_WEIGHTS).fillna(0)

    alliance_col = 'alliance' if 'alliance' in df.columns else None

    if alliance_col and 'matchNumber' in df.columns:
        # Use contributedPoints as alliance total — more accurate than fuel counts
        # and works even when autoFuel/teleFuel aren't collected
        df['_contrib'] = df['contributedPoints'].fillna(0) if 'contributedPoints' in df.columns else 0

        # Alliance total contributed points per match
        alliance_totals = df.groupby(['matchNumber', alliance_col])['_contrib'].sum().reset_index()
        alliance_totals = alliance_totals.rename(columns={'_contrib': 'allianceFuel'})
        df = df.merge(alliance_totals, on=['matchNumber', alliance_col], how='left')

        # Alliance weight sum per match
        alliance_weights = df.groupby(['matchNumber', alliance_col])['tier_weight'].sum().reset_index()
        alliance_weights = alliance_weights.rename(columns={'tier_weight': 'alliance_weight_sum'})
        df = df.merge(alliance_weights, on=['matchNumber', alliance_col], how='left')

        # Proportional Score = allianceFuel × (robot_weight / alliance_weight_sum)
        df['proportional_score'] = df.apply(
            lambda row: round(
                row['allianceFuel'] * (row['tier_weight'] / row['alliance_weight_sum']), 2
            ) if row['alliance_weight_sum'] > 0 else 0.0,
            axis=1
        )
    else:
        df['allianceFuel'] = 0
        df['proportional_score'] = 0.0

    # Drop helper columns not in schema
    for col in ['tier_weight', 'alliance_weight_sum', '_contrib']:
        if col in df.columns:
            df = df.drop(columns=[col])

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

def process_pit_data(csv_path, db_path):
    """Reads a pit scouting CSV and loads it into the pit_data table."""
    print(f" Processing pit file: {csv_path}...")

    try:
        df = pd.read_csv(csv_path)
    except FileNotFoundError:
        print(f"Error: Could not find {csv_path}. Please check the path.")
        return

    conn = sqlite3.connect(db_path)
    cursor = None

    try:
        df.to_sql('temp_pit_data', conn, if_exists='replace', index=False)

        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(pit_data)")
        db_columns = [row[1] for row in cursor.fetchall()]

        common_cols = [c for c in df.columns if c in db_columns]
        if not common_cols:
            print("Error: No matching columns found between pit CSV and pit_data table.")
            return

        cols_string = ", ".join(common_cols)

        upsert_query = f"""
        INSERT OR REPLACE INTO pit_data ({cols_string})
        SELECT {cols_string} FROM temp_pit_data;
        """

        cursor.execute(upsert_query)
        conn.commit()

        print(f"Successfully processed and loaded {len(df)} rows into 'pit_data'!")

    except sqlite3.Error as e:
        print(f"Database error during pit UPSERT: {e}")
    finally:
        if cursor is not None:
            cursor.execute("DROP TABLE IF EXISTS temp_pit_data")
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
