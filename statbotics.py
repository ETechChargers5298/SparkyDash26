import requests

# Check if we are running on the Streamlit Cloud or locally where config.py exists
import streamlit as st
try:
    TBA_API_KEY = st.secrets["TBA_API_KEY"]
    EVENT_KEY = st.secrets["EVENT_KEY"]
    OUR_TEAM = st.secrets["OUR_TEAM"]
except Exception:
    from config import TBA_API_KEY, EVENT_KEY, OUR_TEAM


STATBOTICS_BASE = "https://api.statbotics.io/v3"

@st.cache_data(ttl=120)  # Cache for 2 minutes — EPA updates after each match
def get_event_epas():
    """
    Pulls EPA data for all teams at the current event from Statbotics.
    Returns a dict keyed by team number: {team: {epa_total, epa_auto, epa_teleop, epa_endgame}}
    No API key required.
    """
    try:
        r = requests.get(
            f"{STATBOTICS_BASE}/team_events",
            params={"event": EVENT_KEY, "fields": "team,epa"},
            timeout=5
        )
        if r.status_code != 200:
            return {}

        data = r.json()
        result = {}
        for entry in data:
            team = entry.get("team")
            epa  = entry.get("epa", {})
            if team and epa:
                result[int(team)] = {
                    "epa_total":   epa.get("total_points",   {}).get("mean", 0.0) or 0.0,
                    "epa_auto":    epa.get("auto_points",    {}).get("mean", 0.0) or 0.0,
                    "epa_teleop":  epa.get("teleop_points",  {}).get("mean", 0.0) or 0.0,
                    "epa_endgame": epa.get("endgame_points", {}).get("mean", 0.0) or 0.0,
                }
        return result

    except requests.exceptions.RequestException:
        return {}

def get_team_epa(team_number: int):
    """Returns EPA dict for a single team, or zeros if not found."""
    epas = get_event_epas()
    return epas.get(int(team_number), {
        "epa_total": 0.0, "epa_auto": 0.0,
        "epa_teleop": 0.0, "epa_endgame": 0.0
    })
