import requests

# Check if we are running on the Streamlit Cloud or locally where config.py exists
import streamlit as st
try:
    TBA_API_KEY = st.secrets["TBA_API_KEY"]
    EVENT_KEY = st.secrets["EVENT_KEY"]
    OUR_TEAM = st.secrets["OUR_TEAM"]
except Exception:
    from config import TBA_API_KEY, EVENT_KEY, OUR_TEAM

BASE_URL = "https://www.thebluealliance.com/api/v3"
HEADERS = {"X-TBA-Auth-Key": TBA_API_KEY}

# Three possible states for API responses
API_OK        = "ok"         # Got data successfully
API_NO_DATA   = "no_data"    # Reached API fine but event data doesn't exist yet
API_ERROR     = "error"      # Network failure or bad API key

# ---------------------------------------------------------------------------
# Core fetch helper — returns (status, data) tuple so callers know WHY it failed
# ---------------------------------------------------------------------------
@st.cache_data(ttl=60)
def tba_get(endpoint: str):
    """
    Makes a GET request to the TBA API.
    Returns (status, data) where status is one of API_OK, API_NO_DATA, API_ERROR.
    """
    try:
        r = requests.get(f"{BASE_URL}/{endpoint}", headers=HEADERS, timeout=5)
        if r.status_code == 200:
            data = r.json()
            # TBA returns an empty list or null for events that haven't started
            if data is None or data == [] or data == {}:
                return API_NO_DATA, None
            return API_OK, data
        elif r.status_code in (401, 403):
            # Bad API key
            return API_ERROR, "invalid_key"
        elif r.status_code == 404:
            # Event key doesn't exist or data not posted yet
            return API_NO_DATA, None
        else:
            return API_ERROR, f"HTTP {r.status_code}"
    except requests.exceptions.ConnectionError:
        return API_ERROR, "no_connection"
    except requests.exceptions.Timeout:
        return API_ERROR, "timeout"
    except requests.exceptions.RequestException as e:
        return API_ERROR, str(e)

# ---------------------------------------------------------------------------
# Event-level data
# ---------------------------------------------------------------------------
def get_event_rankings():
    status, data = tba_get(f"event/{EVENT_KEY}/rankings")
    if status == API_OK and data and 'rankings' in data:
        return status, data['rankings']
    return status, []

def get_event_matches():
    status, data = tba_get(f"event/{EVENT_KEY}/matches")
    if status == API_OK and data:
        quals = [m for m in data if m['comp_level'] == 'qm']
        return status, sorted(quals, key=lambda m: m['match_number'])
    return status, []

# ---------------------------------------------------------------------------
# Team-level data
# ---------------------------------------------------------------------------
def get_our_matches():
    status, all_matches = get_event_matches()
    if status != API_OK or not all_matches:
        return status, []
    our_matches = [
        m for m in all_matches
        if f"frc{OUR_TEAM}" in (
            m['alliances']['red']['team_keys'] +
            m['alliances']['blue']['team_keys']
        )
    ]
    return status, our_matches

def get_team_ranking():
    status, rankings = get_event_rankings()
    if status != API_OK or not rankings:
        return status, None
    for r in rankings:
        if r['team_key'] == f"frc{OUR_TEAM}":
            return status, r
    return status, None

# ---------------------------------------------------------------------------
# Match parsing helpers
# ---------------------------------------------------------------------------
def parse_match(match):
    """Extracts a clean flat dict from a raw TBA match object."""
    red_keys  = match['alliances']['red']['team_keys']
    blue_keys = match['alliances']['blue']['team_keys']
    red_score  = match['alliances']['red'].get('score', -1)
    blue_score = match['alliances']['blue'].get('score', -1)
    played = red_score != -1 and blue_score != -1

    our_key = f"frc{OUR_TEAM}"
    if our_key in red_keys:
        our_alliance = 'Red'
        our_score    = red_score
        opp_score    = blue_score
        partners     = [int(k[3:]) for k in red_keys  if k != our_key]
        opponents    = [int(k[3:]) for k in blue_keys]
    else:
        our_alliance = 'Blue'
        our_score    = blue_score
        opp_score    = red_score
        partners     = [int(k[3:]) for k in blue_keys if k != our_key]
        opponents    = [int(k[3:]) for k in red_keys]

    if played:
        result = '✅ Win' if our_score > opp_score else ('❌ Loss' if our_score < opp_score else '➡️ Tie')
    else:
        result = '⏳ Upcoming'

    return {
        'match_number': match['match_number'],
        'our_alliance': our_alliance,
        'partners':     partners,
        'opponents':    opponents,
        'our_score':    our_score if played else '—',
        'opp_score':    opp_score if played else '—',
        'result':       result,
        'played':       played,
        'red_teams':    [int(k[3:]) for k in red_keys],
        'blue_teams':   [int(k[3:]) for k in blue_keys],
    }

# ---------------------------------------------------------------------------
# Human-readable error messages for the UI
# ---------------------------------------------------------------------------
def api_error_message(error_detail: str) -> str:
    messages = {
        "invalid_key":   "❌ Invalid API key. Check `TBA_API_KEY` in config.py and make sure you copied it correctly from thebluealliance.com/account.",
        "no_connection": "❌ No internet connection. Connect to Wi-Fi and hit 🔄 Refresh Data.",
        "timeout":       "❌ TBA API timed out. The server may be busy — try again in a moment.",
    }
    return messages.get(error_detail, f"❌ Unexpected API error: {error_detail}")
