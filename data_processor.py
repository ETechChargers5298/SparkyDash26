import pandas as pd
import sqlite3
from pathlib import Path

# --- THE PIPELINE ---
def process_match_data(csv_path, db_path):
    """Reads the raw CSV, transforms the data, and loads it into SQLite."""
    print(f" Processing file: {csv_path}...")

    # 1. Extract
    try:
        df = pd.read_csv(csv_path)
    except FileNotFoundError:
        print(f"Error: Could not find {csv_path}. Please check the path.")
        return

    # 2. Normalize Scoutradioz column names
    # team_key comes as "frc1234" — strip prefix, rename to teamNumber
    if 'team_key' in df.columns and 'teamNumber' not in df.columns:
        df['teamNumber'] = df['team_key'].str.replace('frc', '', regex=False).astype(int)

    # match_number → matchNumber
    if 'match_number' in df.columns and 'matchNumber' not in df.columns:
        df = df.rename(columns={'match_number': 'matchNumber'})

    # 3. Strip percentage descriptions from quality fields
    # Scoutradioz exports "Perfect: 95-100%" — we only store "Perfect"
    for col in ['volleyQuality', 'autoVolleyQuality']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.split(':').str[0].str.strip()

    # Strip description from robotTier just in case
    if 'robotTier' in df.columns:
        df['robotTier'] = df['robotTier'].astype(str).str.split(':').str[0].str.strip()

    # 4. Recalculate contributedPoints using quality scores and climb points
    # Quality score mapping
    QUALITY_MAP = {
        'Perfect':       1.000,
        'Above Average': 0.750,
        'Average':       0.500,
        'Below Average': 0.125,
    }
    CLIMB_POINTS = {
        'Level 1': 10,
        'Level 2': 20,
        'Level 3': 30,
        'None':     0,
    }

    def quality_score(val):
        return QUALITY_MAP.get(str(val).strip(), 0.5)

    def climb_points(val):
        return CLIMB_POINTS.get(str(val).strip(), 0)

    tele_quality  = df['volleyQuality'].apply(quality_score)     if 'volleyQuality'     in df.columns else 0.5
    auto_quality  = df['autoVolleyQuality'].apply(quality_score) if 'autoVolleyQuality' in df.columns else 0.5
    tele_volleys  = df['volleysAttempted'].fillna(0)             if 'volleysAttempted'  in df.columns else 0
    auto_volleys  = df['autoVolleysAttempted'].fillna(0)         if 'autoVolleysAttempted' in df.columns else 0
    climb_pts     = df['teleClimb'].apply(climb_points)          if 'teleClimb'         in df.columns else 0

    df['contributedPoints'] = (
        (tele_volleys * tele_quality * 20) +
        (auto_volleys * auto_quality * 20) +
        climb_pts
    ).round(0).astype(int)

    # 5. Proportional Score
    TIER_WEIGHTS = {'Elite': 4, 'High': 3, 'Medium': 2, 'Low': 1, 'None': 0}

    if 'robotTier' not in df.columns:
        df['robotTier'] = 'None'
    df['robotTier']   = df['robotTier'].fillna('None').astype(str).str.strip()
    df['tier_weight'] = df['robotTier'].map(TIER_WEIGHTS).fillna(0)

    alliance_col = 'alliance' if 'alliance' in df.columns else None

    if alliance_col and 'matchNumber' in df.columns:
        df['_contrib'] = df['contributedPoints'].fillna(0)

        alliance_totals = df.groupby(['matchNumber', alliance_col])['_contrib'].sum().reset_index()
        alliance_totals = alliance_totals.rename(columns={'_contrib': 'allianceFuel'})
        df = df.merge(alliance_totals, on=['matchNumber', alliance_col], how='left')

        alliance_weights = df.groupby(['matchNumber', alliance_col])['tier_weight'].sum().reset_index()
        alliance_weights = alliance_weights.rename(columns={'tier_weight': 'alliance_weight_sum'})
        df = df.merge(alliance_weights, on=['matchNumber', alliance_col], how='left')

        df['proportional_score'] = df.apply(
            lambda row: round(
                row['allianceFuel'] * (row['tier_weight'] / row['alliance_weight_sum']), 2
            ) if row['alliance_weight_sum'] > 0 else 0.0,
            axis=1
        )
    else:
        df['allianceFuel']       = 0
        df['proportional_score'] = 0.0

    # Drop helper columns not in schema
    for col in ['tier_weight', 'alliance_weight_sum', '_contrib']:
        if col in df.columns:
            df = df.drop(columns=[col])

    # Fill missing values
    num_cols = df.select_dtypes(include=['float64', 'int64']).columns
    df[num_cols] = df[num_cols].fillna(0)
    df = df.fillna('N/A')

    # 6. Load via UPSERT
    conn = sqlite3.connect(db_path)
    cursor = None

    try:
        df.to_sql('temp_match_data', conn, if_exists='replace', index=False)

        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(match_data)")
        db_columns = [row[1] for row in cursor.fetchall()]

        common_cols = [c for c in df.columns if c in db_columns]
        cols_string = ", ".join(common_cols)

        cursor.execute(f"""
        INSERT OR REPLACE INTO match_data ({cols_string})
        SELECT {cols_string} FROM temp_match_data;
        """)
        conn.commit()
        print(f"Successfully processed and loaded {len(df)} rows into 'match_data'!")

    except sqlite3.Error as e:
        print(f"Database error during UPSERT: {e}")
    finally:
        if cursor is not None:
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

    # Normalize team_key → teamNumber
    if 'team_key' in df.columns and 'teamNumber' not in df.columns:
        df['teamNumber'] = df['team_key'].str.replace('frc', '', regex=False).astype(int)

    # Strip descriptions from driveBaseType
    # Scoutradioz exports "Swerve: Moves and turns freely 360 degrees" — we only store "Swerve"
    if 'driveBaseType' in df.columns:
        df['driveBaseType'] = df['driveBaseType'].astype(str).str.split(':').str[0].str.strip()

    # Fill missing values
    num_cols = df.select_dtypes(include=['float64', 'int64']).columns
    df[num_cols] = df[num_cols].fillna(0)
    df = df.fillna('N/A')

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
        cursor.execute(f"""
        INSERT OR REPLACE INTO pit_data ({cols_string})
        SELECT {cols_string} FROM temp_pit_data;
        """)
        conn.commit()
        print(f"Successfully processed and loaded {len(df)} rows into 'pit_data'!")

    except sqlite3.Error as e:
        print(f"Database error during pit UPSERT: {e}")
    finally:
        if cursor is not None:
            cursor.execute("DROP TABLE IF EXISTS temp_pit_data")
        conn.close()


if __name__ == "__main__":
    project_root = Path(__file__).resolve().parent
    match_csv_file = project_root / 'data' / 'match_export.csv'
    pit_csv_file   = project_root / 'data' / 'pit_export.csv'
    database_file  = project_root / 'database' / 'scouting_2026.db'

    (project_root / 'data').mkdir(parents=True, exist_ok=True)
    (project_root / 'database').mkdir(parents=True, exist_ok=True)

    process_match_data(match_csv_file, database_file)
    process_pit_data(pit_csv_file, database_file)
