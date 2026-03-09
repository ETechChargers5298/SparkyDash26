import sqlite3
import os

def create_database():
    # 1. Ensure the directory exists so SQLite doesn't throw a path error
    os.makedirs('database', exist_ok=True)
    db_path = 'database/scouting_2026.db'
    
    # 2. Connect to the database (this creates the file if it doesn't exist)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 3. Create the Match Data Table
    # We use IF NOT EXISTS so you can run this script safely without overwriting existing data
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS match_data (
        -- Composite Primary Key: Prevents duplicate entries if syncing the same CSV twice
        matchNumber INTEGER,
        teamNumber INTEGER,
        
        -- Auto
        startPosition TEXT,
        autoFuel INTEGER,
        autoFeed INTEGER,
        autoClimbLevel TEXT,
        autoClimbPos TEXT,
        crossBumpAuto BOOLEAN,
        crossTrenchAuto BOOLEAN,
        autoOofTime INTEGER,
        
        -- Teleop
        teleFuel INTEGER,
        teleFeed INTEGER,
        crossBumpTele BOOLEAN,
        crossTrenchTele BOOLEAN,
        teleOofTime INTEGER,
        attackingTime INTEGER,
        defendedTime INTEGER,
        scoringLocations TEXT,
        feedingLocations TEXT,
        
        -- Endgame & Qualitative
        teleClimb TEXT,
        climbTime INTEGER,
        drivetrainSpeed TEXT,
        driverSkill TEXT,
        freeClimbPenalty BOOLEAN,
        matchNotes TEXT,
        
        -- Raw SPR Estimate from Scoutradioz
        contributedPoints INTEGER,
        
        -- Calculated Fields (These will be populated later by a seperate script)
        auto_bps REAL DEFAULT 0.0,
        tele_bps REAL DEFAULT 0.0,
        
        PRIMARY KEY (matchNumber, teamNumber)
    )
    ''')

    # 4. Create the Pit Data Table
    # Kept in a separate table to keep the match data clean and normalized. 
    # We will JOIN this in Streamlit later.
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS pit_data (
        teamNumber INTEGER PRIMARY KEY,
        driverExp TEXT,
        autoStartPref TEXT,
        driverStationPref TEXT,
        robotStrategy TEXT,
        bestAutoDescription TEXT,
        driveTrain TEXT,
        robotWidth REAL,
        robotLength REAL,
        robotHeight REAL,
        extendsFrame BOOLEAN,
        extensionDir TEXT,
        canRetract BOOLEAN,
        climbAbility TEXT,
        intakeSource TEXT,
        shotMobility TEXT,
        shotRange TEXT,
        pitNotes TEXT
    )
    ''')

    # Commit changes and close the connection
    conn.commit()
    conn.close()
    print(f"Database initialized successfully at {db_path}")
    print("Tables created: 'match_data' and 'pit_data'.")

if __name__ == "__main__":
    create_database()
