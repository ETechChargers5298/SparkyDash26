import sqlite3
from pathlib import Path

def create_database():
    # 1. Ensure the database directory exists
    project_root = Path(__file__).resolve().parent
    db_path = project_root / 'database' / 'scouting_2026.db'
    (project_root / 'database').mkdir(parents=True, exist_ok=True)

    # 2. Connect — SQLite creates the file if it doesn't exist
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 3. Match Data Table
    # Composite PRIMARY KEY prevents duplicate entries for the same robot in the same match.
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS match_data (

        -- Identity
        match_number INTEGER,
        team_number  INTEGER,

        -- Auto Period
        start_position         TEXT,
        auto_herding_push_wave   INTEGER DEFAULT 0,
        auto_herding_spit_wave   INTEGER DEFAULT 0,
        auto_herding_launch_wave INTEGER DEFAULT 0,
        auto_volleys_attempted  INTEGER DEFAULT 0,
        auto_volley_quality     TEXT    DEFAULT 'Average',
        auto_climb_level        TEXT,
        auto_climb_pos          TEXT,
        cross_bump_auto         BOOLEAN,
        cross_trench_auto       BOOLEAN,
        auto_breakdown         BOOLEAN DEFAULT 0,
        auto_breakdown_des      TEXT,

        -- Teleop Period
        tele_herding_push_wave   INTEGER DEFAULT 0,
        tele_herding_spit_wave   INTEGER DEFAULT 0,
        tele_herding_launch_wave INTEGER DEFAULT 0,
        tele_volleys_attempted      INTEGER DEFAULT 0,
        tele_volley_quality         TEXT    DEFAULT 'Average',
        tele_feed              INTEGER DEFAULT 0,
        cross_bump_tele         BOOLEAN,
        cross_trench_tele       BOOLEAN,
        defended_time          BOOLEAN,
        scoring_locations      TEXT,
        feeding_locations      TEXT,

        -- Endgame & Qualitative
        tele_climb        TEXT,
        climb_time        INTEGER,
        drivebase_speed  TEXT,
        driver_skill      TEXT,

        -- Metrics
        robot_tier         TEXT    DEFAULT 'None',
        contributed_points INTEGER DEFAULT 0,

        -- Penalties & Notes
        free_climb_penalty BOOLEAN,
        tele_breakdown    BOOLEAN DEFAULT 0,
        tele_breakdown_des TEXT,
        match_notes       TEXT,

        -- Calculated by data_processor.py during ETL
        alliance_fuel       INTEGER DEFAULT 0,
        proportional_score REAL    DEFAULT 0.0,

        PRIMARY KEY (match_number, team_number)
    )
    ''')

    # 4. Pit Data Table — matches pit scouting form exactly
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS pit_data (

        team_number INTEGER PRIMARY KEY,
        team_key                INTEGER DEFAULT 0,

        -- Driver & Strategy
        driver_exp      TEXT,
        auto_start_pref       TEXT,
        driver_station_pref     TEXT,
        auto_robo_strat  TEXT,
        robo_strat      TEXT,
        robo_best_auto   REAL    DEFAULT 0,

        -- Drive Base
        drivebase_type  TEXT,
        drivebase_notes TEXT,

        -- Robot Dimensions
        robot_width     REAL,
        robot_length    REAL,
        robot_height    REAL,

        -- Robot Specs
        extendable      BOOLEAN,
        extend_multi_dir  BOOLEAN,
        use_turret       BOOLEAN,
        num_turrets INTEGER DEFAULT 0,
        volley_amount    INTEGER DEFAULT 0,
        hopper_capacity  INTEGER DEFAULT 0,
        use_vision       BOOLEAN,

        -- Climb Capabilities
        climb   BOOLEAN,
        l1_auto  BOOLEAN,
        l1_climb BOOLEAN,
        l2_climb BOOLEAN,
        l3_climb BOOLEAN,

        -- Intake Capabilities
        ground_intake  BOOLEAN,
        hp_intake BOOLEAN,
        depot_intake  BOOLEAN,

        -- Shooting
        move_shoot  BOOLEAN,
        shoot_area  TEXT,

        -- General
        robot_des TEXT,
        extra            TEXT
    )
    ''')

    # 5. Save and close
    conn.commit()
    conn.close()
    print(f"Database initialized successfully at {db_path}")
    print("Tables created: 'match_data' and 'pit_data'.")

if __name__ == "__main__":
    create_database()
