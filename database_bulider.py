import sqlite3
import os

def create_database():
    os.makedirs('database', exist_ok=True)
    db_path = 'database/scouting_2026.db'
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
                   
                   CREATE TABLE IF NOT EXIST match_data(
                       matchNumber INTEGER, 
                       teamNumber INTEGER,
                       startPosition TEXT,
                       autoFuel INTEGER,
                       autoFeed INTEGER,
                       autoClimbLevel TEXT,
                       autoClimbPosition TEXT,
                       crossBumpAuto BOOLEAN,
                       crossTrenchAuto BOOLEAN,
                       autoOofTime INTEGER,
                       teleFuel INTEGER,
                       teleFeed INTEGER,
                       crossBumpTele BOOLEAN,
                       crossTrenchTele BOOLEAN,
                       scoringLocations TEXT,
                       feedingLocations TEXT,
                       teleClimb TEXT,
                       climbTime INTEGER,
                       driveTrainSpeed TEXT,
                       driverSkill TEXT,
                       matchNotes TEXT,
                       PRIMARY KEY (matchNumber, teamNumber)
                   )
                   
                   ''')
    
    cursor.execute('''
    
    CREATE TABLE IF NOT EXIST pit_data(
        
        teamNumber INTGER PRIMARTY KEY,
        driveEXP TEXT,
        autoStartPref TEXT,
        driverStationPref TEXT,
        robotStrat TEXT,
        bestAutoDescription TEXT,
        driveTrain Text,
        robotWidth REAL,
        robotLength REAL,
        robotHeight REAL,
        extendsFRAME BOOLEAN,
        extenstionDirection TEXT,
        canRetract BOOLEAN,
        climbAblity TEXT,
        intakeSource TEXT,
        shotMoblity TEXT,
        shotRange TEXT,
        pitNotes TEXT,
        
    )
    
    ''')
    
    conn.commit()
    conn.close()
    print("database created successfully")

