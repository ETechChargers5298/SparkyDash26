import pandas as pd
import sqlite3
from pathlib import Path

# Mapping from Scoutradioz camelCase export fields → snake_case DB columns
MATCH_COLUMN_MAP = {
    # Identity
    'team_number':            'team_number',       # already set from team_key
    'match_number':           'match_number',       # already set from match_number
    # Auto
    'startPosition':          'start_position',
    'autoHerdingPushWave':    'auto_herding_push_wave',
    'autoHerdingSpitWave':    'auto_herding_spit_wave',
    'autoHerdingLaunchWave':  'auto_herding_launch_wave',
    'autoVolleys':            'auto_volleys',
    'autoVolleyQuality':      'auto_volley_quality',
    'autoClimbLevel':         'auto_climb_level',
    'autoClimbPos':           'auto_climb_pos',
    'crossBumpAuto':          'cross_bump_auto',
    'crossTrenchAuto':        'cross_trench_auto',
    'autoBreakDown':          'auto_break_down',
    'autoBreakDownDes':       'auto_break_down_des',
    # Teleop
    'teleHerdingPushWave':    'tele_herding_push_wave',
    'teleHerdingSpitWave':    'tele_herding_spit_wave',
    'teleHerdingLaunchWave':  'tele_herding_launch_wave',
    'teleVolleys':            'tele_volleys',
    'teleVolleyQuality':      'tele_volley_quality',
    'teleFeed':               'tele_feed',
    'crossBumpTele':          'cross_bump_tele',
    'crossTrenchTele':        'cross_trench_tele',
    'defendedTime':           'defended_time',
    'scoringLocations':       'scoring_locations',
    'feedingLocations':       'feeding_locations',
    # Endgame
    'teleClimb':              'tele_climb',
    'climbTime':              'climb_time',
    'drivebaseSpeed':         'drivebase_speed',
    'driverSkill':            'driver_skill',
    # Metrics
    'robotTier':              'robot_tier',
    'contributedPoints':      'contributed_points',
    # Penalties & Notes
    'freeClimbPenalty':       'free_climb_penalty',
    'teleBreakdown':          'tele_breakdown',
    'teleBreakdownDes':       'tele_breakdown_des',
    'matchNotes':             'match_notes',
}

PIT_COLUMN_MAP = {
    'teamNumber':       'team_number',   # already set from team_key
    'driverexp':        'driver_exp',
    'autoStartPref':    'auto_start_pref',
    'driverPref':       'driver_pref',
    'autoRoboStrat':    'auto_robo_strat',
    'roboStrat':        'robo_strat',
    'roboBestAuto':     'robo_best_auto',
    'drivebaseType':    'drivebase_type',
    'drivebaseNotes':   'drivebase_notes',
    'robotWidth':       'robot_width',
    'robotLength':      'robot_length',
    'robotHeight':      'robot_height',
    'extendable':       'extendable',
    'extendMultiDir':   'extend_multi_dir',
    'useTurret':        'use_turret',
    'numberOfTurrets':  'num_turrets',
    'volleyAmount':     'volley_amount',
    'hopperCapacity':   'hopper_capacity',
    'useVision':        'use_vision',
    'climbAbility':     'climb_ability',
    'l1Auto':           'l1_auto',
    'l1Climb':          'l1_climb',
    'l2Climb':          'l2_climb',
    'l3Climb':          'l3_climb',
    'groundIntake':     'ground_intake',
    'hpIntake':         'hp_intake',
    'depotIntake':      'depot_intake',
    'moveShoot':        'move_shoot',
    'shootArea':        'shoot_area',
    'robotDes':         'robot_des',
    'pitNotes':         'pit_notes',
}

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

TIER_WEIGHTS = {'Elite': 4, 'High': 3, 'Medium': 2, 'Low': 1, 'None': 0}


def process_match_data(csv_path, db_path):
    """Reads the raw Scoutradioz CSV, transforms, maps columns, and loads into SQLite."""
    print(f" Processing file: {csv_path}...")

    try:
        df = pd.read_csv(csv_path)
    except FileNotFoundError:
        print(f"Error: Could not find {csv_path}.")
        return

    # 1. Normalize identity columns
    if 'team_key' in df.columns and 'team_number' not in df.columns:
        df['team_number'] = df['team_key'].str.replace('frc', '', regex=False).astype(int)
    if 'match_number' in df.columns:
        df = df.rename(columns={'match_number': 'match_number'})

    # 2. Strip percentage descriptions from quality fields
    # Scoutradioz exports "Perfect: 95-100%" — we only store "Perfect"
    for col in ['teleVolleyQuality', 'autoVolleyQuality']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.split(':').str[0].str.strip()

    # Strip descriptions from robotTier and driveBaseType just in case
    for col in ['robotTier']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.split(':').str[0].str.strip()

    # 3. Recalculate contributed_points using quality scores + climb points
    def quality_score(val):
        return QUALITY_MAP.get(str(val).strip(), 0.5)

    def climb_pts(val):
        return CLIMB_POINTS.get(str(val).strip(), 0)

    tele_quality = df['teleVolleyQuality'].apply(quality_score)     if 'televolleyQuality'  in df.columns else 0.5
    auto_quality = df['autoVolleyQuality'].apply(quality_score)     if 'autoVolleyQuality'  in df.columns else 0.5
    tele_volleys = df['teleVolleys'].fillna(0)                      if 'televolleys'        in df.columns else 0
    auto_volleys = df['autoVolleys'].fillna(0)                      if 'autoVolleys'        in df.columns else 0
    climb        = df['teleClimb'].apply(climb_pts)                 if 'teleClimb'          in df.columns else 0

    df['contributedPoints'] = (
        (tele_volleys * tele_quality * 20) +
        (auto_volleys * auto_quality * 20) +
        climb
    ).round(0).astype(int)

    # 4. Proportional Score
    if 'robotTier' not in df.columns:
        df['robotTier'] = 'None'
    df['robotTier']   = df['robotTier'].fillna('None').astype(str).str.strip()
    df['tier_weight'] = df['robotTier'].map(TIER_WEIGHTS).fillna(0)

    alliance_col = 'alliance' if 'alliance' in df.columns else None

    if alliance_col and 'match_number' in df.columns:
        df['_contrib'] = df['contributedPoints'].fillna(0)

        alliance_totals = df.groupby(['match_number', alliance_col])['_contrib'].sum().reset_index()
        alliance_totals = alliance_totals.rename(columns={'_contrib': 'alliance_fuel'})
        df = df.merge(alliance_totals, on=['match_number', alliance_col], how='left')

        alliance_weights = df.groupby(['match_number', alliance_col])['tier_weight'].sum().reset_index()
        alliance_weights = alliance_weights.rename(columns={'tier_weight': 'alliance_weight_sum'})
        df = df.merge(alliance_weights, on=['match_number', alliance_col], how='left')

        df['proportional_score'] = df.apply(
            lambda row: round(
                row['alliance_fuel'] * (row['tier_weight'] / row['alliance_weight_sum']), 2
            ) if row['alliance_weight_sum'] > 0 else 0.0,
            axis=1
        )
    else:
        df['alliance_fuel']      = 0
        df['proportional_score'] = 0.0

    # Drop helper columns
    for col in ['tier_weight', 'alliance_weight_sum', '_contrib']:
        if col in df.columns:
            df = df.drop(columns=[col])

    # 5. Rename camelCase Scoutradioz columns → snake_case DB columns
    df = df.rename(columns={k: v for k, v in MATCH_COLUMN_MAP.items() if k in df.columns})

    # 6. Fill missing values
    num_cols = df.select_dtypes(include=['float64', 'int64']).columns
    df[num_cols] = df[num_cols].fillna(0)
    df = df.fillna('N/A')

    # 7. UPSERT into DB
    conn = sqlite3.connect(db_path)
    cursor = None
    try:
        df.to_sql('temp_match_data', conn, if_exists='replace', index=False)
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(match_data)")
        db_columns = [row[1] for row in cursor.fetchall()]
        common_cols  = [c for c in df.columns if c in db_columns]
        cols_string  = ", ".join(common_cols)
        cursor.execute(f"""
        INSERT OR REPLACE INTO match_data ({cols_string})
        SELECT {cols_string} FROM temp_match_data;
        """)
        conn.commit()
        print(f"Successfully loaded {len(df)} rows into 'match_data'!")
    except sqlite3.Error as e:
        print(f"Database error: {e}")
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
        print(f"Error: Could not find {csv_path}.")
        return

    # Normalize team_key → team_number
    if 'team_key' in df.columns and 'team_number' not in df.columns:
        df['team_number'] = df['team_key'].str.replace('frc', '', regex=False).astype(int)

    # Strip descriptions from driveBaseType
    if 'driveBaseType' in df.columns:
        df['driveBaseType'] = df['driveBaseType'].astype(str).str.split(':').str[0].str.strip()

    # Rename camelCase Scoutradioz columns → snake_case DB columns
    df = df.rename(columns={k: v for k, v in PIT_COLUMN_MAP.items() if k in df.columns})

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
        db_columns  = [row[1] for row in cursor.fetchall()]
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
        print(f"Successfully loaded {len(df)} rows into 'pit_data'!")
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    finally:
        if cursor is not None:
            cursor.execute("DROP TABLE IF EXISTS temp_pit_data")
        conn.close()


if __name__ == "__main__":
    project_root   = Path(__file__).resolve().parent
    match_csv_file = project_root / 'data' / 'match_export.csv'
    pit_csv_file   = project_root / 'data' / 'pit_export.csv'
    database_file  = project_root / 'database' / 'scouting_2026.db'

    (project_root / 'data').mkdir(parents=True, exist_ok=True)
    (project_root / 'database').mkdir(parents=True, exist_ok=True)

    process_match_data(match_csv_file, database_file)
    process_pit_data(pit_csv_file, database_file)
