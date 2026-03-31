import sqlite3
from pathlib import Path

def create_database():
    # 1. Ensure the database directory exists
    # Path(__file__).resolve().parent gives us the folder this script lives in,
    # so the database/ subfolder is always created in the right place regardless
    # of where you run the script from.
    project_root = Path(__file__).resolve().parent
    db_path = project_root / 'database' / 'scouting_2026.db'
    (project_root / 'database').mkdir(parents=True, exist_ok=True)

    # 2. Connect to the database
    # SQLite creates the .db file automatically if it doesn't exist yet.
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 3. Create the Match Data Table
    # IF NOT EXISTS means this script is safe to re-run — it won't wipe existing data.
    # The composite PRIMARY KEY on (matchNumber, teamNumber) ensures we can never
    # accidentally insert two entries for the same robot in the same match.
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS match_data (

        -- Identity
        matchNumber INTEGER,
        teamNumber  INTEGER,

        -- Auto Period
        startPosition        TEXT,
        autoVolleysAttempted INTEGER DEFAULT 0,
        autoVolleyQuality    TEXT    DEFAULT 'Average',
        autoFuel        INTEGER,
        autoFeed        INTEGER,
        autoClimbLevel  TEXT,
        autoClimbPos    TEXT,
        crossBumpAuto   BOOLEAN,
        crossTrenchAuto BOOLEAN,
        autoOofTime     INTEGER,

        -- Teleop Period
        teleFuel        INTEGER,
        teleFeed        INTEGER,
        crossBumpTele   BOOLEAN,
        crossTrenchTele BOOLEAN,
        teleOofTime     INTEGER,
        defendedTime    BOOLEAN,
        scoringLocations TEXT,
        feedingLocations TEXT,

        -- Volley Tracking (collected by scouts in Scoutradioz)
        volleysAttempted INTEGER DEFAULT 0,
        volleyQuality    TEXT    DEFAULT 'Average',

        -- Endgame & Qualitative
        teleClimb        TEXT,
        climbTime        INTEGER,
        drivetrainSpeed  TEXT,
        driverSkill      TEXT,
        freeClimbPenalty BOOLEAN,
        matchNotes       TEXT,

        -- Scoutradioz SPR Estimate
        contributedPoints INTEGER,

        -- Scout-rated performance tier per match (Elite / High / Medium / Low / None)
        -- Set by scouts in Scoutradioz. Used to calculate proportional_score.
        robotTier TEXT DEFAULT 'None',

        -- Fields calculated by data_processor.py during ETL
        allianceFuel      INTEGER DEFAULT 0,
        proportional_score REAL    DEFAULT 0.0,

        PRIMARY KEY (matchNumber, teamNumber)
    )
    ''')

    # 4. Create the Pit Data Table
    # Kept separate from match_data so the per-match table stays clean.
    # Joined in Streamlit when needed via teamNumber.
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS pit_data (
        teamNumber        INTEGER PRIMARY KEY,
        driverExp         TEXT,
        autoStartPref     TEXT,
        driverStationPref TEXT,
        robotStrategy     TEXT,
        bestAutoDescription TEXT,
        driveTrain        TEXT,
        robotWidth        REAL,
        robotLength       REAL,
        robotHeight       REAL,
        extendsFrame      BOOLEAN,
        extensionDir      TEXT,
        canRetract        BOOLEAN,
        climbAbility      TEXT,
        intakeSource      TEXT,
        shotMobility      TEXT,
        shotRange         TEXT,
        pitNotes          TEXT
    )
    ''')

    # 5. Save and close
    conn.commit()
    conn.close()
    print(f"Database initialized successfully at {db_path}")
    print("Tables created: 'match_data' and 'pit_data'.")

if __name__ == "__main__":
    create_database()
