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
        matchNumber INTEGER,
        teamNumber  INTEGER,

        -- Auto Period
        startPosition         TEXT,
        autoHerdingPushWave   INTEGER DEFAULT 0,
        autoHerdingSpitWave   INTEGER DEFAULT 0,
        autoHerdingLaunchWave INTEGER DEFAULT 0,
        autoVolleysAttempted  INTEGER DEFAULT 0,
        autoVolleyQuality     TEXT    DEFAULT 'Average',
        autoClimbLevel        TEXT,
        autoClimbPos          TEXT,
        crossBumpAuto         BOOLEAN,
        crossTrenchAuto       BOOLEAN,
        autoBreakDown         BOOLEAN DEFAULT 0,
        autoBreakDownDes      TEXT,

        -- Teleop Period
        teleHerdingPushWave   INTEGER DEFAULT 0,
        teleHerdingSpitWave   INTEGER DEFAULT 0,
        teleHerdingLaunchWave INTEGER DEFAULT 0,
        volleysAttempted      INTEGER DEFAULT 0,
        volleyQuality         TEXT    DEFAULT 'Average',
        teleFeed              INTEGER DEFAULT 0,
        crossBumpTele         BOOLEAN,
        crossTrenchTele       BOOLEAN,
        defendedTime          BOOLEAN,
        scoringLocations      TEXT,
        feedingLocations      TEXT,

        -- Endgame & Qualitative
        teleClimb        TEXT,
        climbTime        INTEGER,
        drivetrainSpeed  TEXT,
        driverSkill      TEXT,

        -- Metrics
        robotTier         TEXT    DEFAULT 'None',
        contributedPoints INTEGER DEFAULT 0,

        -- Penalties & Notes
        freeClimbPenalty BOOLEAN,
        teleBreakDown    BOOLEAN DEFAULT 0,
        teleBreakDownDes TEXT,
        matchNotes       TEXT,

        -- Calculated by data_processor.py during ETL
        allianceFuel       INTEGER DEFAULT 0,
        proportional_score REAL    DEFAULT 0.0,

        PRIMARY KEY (matchNumber, teamNumber)
    )
    ''')

    # 4. Pit Data Table — matches pit scouting form exactly
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS pit_data (

        teamNumber INTEGER PRIMARY KEY,

        -- Driver & Strategy
        driverexp      TEXT,
        autoPref       TEXT,
        driverPref     TEXT,
        autoRoboStrat  TEXT,
        roboStrat      TEXT,
        roboBestAuto   REAL    DEFAULT 0,

        -- Drive Base
        driveBaseType  TEXT,
        driveBaseNotes TEXT,

        -- Robot Dimensions
        robotWidth     REAL,
        robotLength    REAL,
        robotHeight    REAL,

        -- Robot Specs
        extendable      BOOLEAN,
        extendMultiDir  BOOLEAN,
        useTurret       BOOLEAN,
        numberOfTurrets INTEGER DEFAULT 0,
        volleyAmount    INTEGER DEFAULT 0,
        hopperCapacity  INTEGER DEFAULT 0,
        useVision       BOOLEAN,

        -- Climb Capabilities
        Climb   BOOLEAN,
        L1Auto  BOOLEAN,
        L1Climb BOOLEAN,
        L2Climb BOOLEAN,
        L3Climb BOOLEAN,

        -- Intake Capabilities
        gIntake  BOOLEAN,
        HPIntake BOOLEAN,
        dIntake  BOOLEAN,

        -- Shooting
        moveShoot  BOOLEAN,
        shootArea  TEXT,

        -- General
        robotDescription TEXT,
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
