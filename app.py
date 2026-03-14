import os
import pandas as pd
import sqlite3 as sq
import streamlit as st
import plotly.express as ex

st.set_page_config(page_title ="E-TECH CHAGRGERS SCOUTING DASHBOARD", layout = "wide")
@st.cache_data
def load_data(query):
    query = """
    SELECT
    teamNumber, COUNT(matchNumber) as matches_played
    , AVG(auto_bps) as avg_auto_bps
    , AVG(tele_bps) as avg_tele_bps
    , AVG(autoFuel + teleFuel) as avg_total_fuel
    , AVG(teleAccuracy) as avg_accuracy
    , AVG(defendedTime) as avg_defended_time
    FROM match_data
    GROUP BY teamNumber
    """
    conn = sq.connect('database/scouting_2026.db')
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

st.sidebar.title("Rebuilt 2026 strategy")
st.sidebar.markdown("---")
page = st.sidebar.radio("Navigation", ["Pre-match predictor", "Picklist", "Team deep-dive"])
if page == "Pre-match predictor":
    
    st.title("Pre-match strategy board")
    st.markdown("compare upcoming alliances")