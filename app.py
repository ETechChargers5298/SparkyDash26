import tempfile
import numpy as np
import pandas as pd
import sqlite3 as sq
import streamlit as st
import plotly.express as px
from pathlib import Path
from data_processor import process_match_data, process_pit_data
from TBA import get_our_matches, get_team_ranking, get_event_rankings, get_event_matches, parse_match, api_error_message, API_OK, API_NO_DATA, API_ERROR
from config import OUR_TEAM, EVENT_KEY
from statbotics import get_event_epas, get_team_epa

# --- CONFIG ---
st.set_page_config(page_title="E-TECH CHARGERS SCOUTING DASHBOARD", layout="wide")

DB_PATH = Path(__file__).resolve().parent / 'database' / 'scouting_2026.db'

# =============================================================================
# DATA LOADERS
# =============================================================================
@st.cache_data
def load_team_averages():
    """Loads per-team aggregated stats with volleys, FER, contributed points, EPA, and tier."""
    query = """
    SELECT
        teamNumber,
        COUNT(matchNumber)                              AS matches_played,
        AVG(contributedPoints)                          AS avg_contributed_points,
        -- defendedTime is now boolean: % of matches where robot was defended
        ROUND(AVG(CAST(defendedTime AS REAL)), 3)       AS defended_pct,
        AVG(autoVolleysAttempted)                       AS avg_auto_volleys_attempted,

        -- Auto Volley Quality Score
        ROUND(AVG(
            CASE autoVolleyQuality
                WHEN 'Perfect'       THEN 1.000
                WHEN 'Above Average' THEN 0.750
                WHEN 'Average'       THEN 0.500
                WHEN 'Below Average' THEN 0.125
                ELSE                      0.500
            END
        ), 3)                                           AS auto_volley_quality_score,

        AVG(volleysAttempted)                           AS avg_volleys_attempted,

        -- Tele Volley Quality Score
        ROUND(AVG(
            CASE volleyQuality
                WHEN 'Perfect'       THEN 1.000
                WHEN 'Above Average' THEN 0.750
                WHEN 'Average'       THEN 0.500
                WHEN 'Below Average' THEN 0.125
                ELSE                      0.500
            END
        ), 3)                                           AS volley_quality_score,

        -- Shift Efficiency: avg volleys attempted per active shift
        -- Each match has 4 active shifts (25 seconds each) for each alliance
        ROUND(
            AVG(volleysAttempted) * 1.0 / 4.0, 3
        )                                               AS shift_efficiency,

        -- Climb reliability
        ROUND(
            SUM(CASE WHEN teleClimb != 'None' AND teleClimb != 'N/A' AND teleClimb != '' THEN 1 ELSE 0 END)
            * 1.0 / COUNT(matchNumber), 3
        )                                               AS climb_reliability,

        -- Proportional Score: avg alliance-weighted contribution
        AVG(proportional_score)                         AS avg_proportional_score,

        -- Most common scout-rated tier
        robotTier                                       AS scout_tier
    FROM match_data
    WHERE teamNumber IS NOT NULL
    GROUP BY teamNumber
    """
    conn = sq.connect(DB_PATH)
    df = pd.read_sql_query(query, conn)
    conn.close()

    if not df.empty:
        # Consistency via stddev of contributedPoints
        df['_cp'] = df['avg_contributed_points'].fillna(0.0)
        df['_cp_sq'] = (df['_cp'] ** 2)
        # Use sample variance across matches approximated from avg
        df['tele_fuel_stddev'] = df['_cp'] * 0.3  # rough approximation
        df = df.drop(columns=['_cp', '_cp_sq'], errors='ignore')

        # Fill remaining nulls
        for col in ['avg_contributed_points', 'shift_efficiency', 'volley_quality_score',
                    'auto_volley_quality_score', 'climb_reliability',
                    'avg_volleys_attempted', 'avg_auto_volleys_attempted']:
            df[col] = df[col].fillna(0.0)

        # Merge Statbotics EPA
        epas = get_event_epas()
        df['epa_total']   = df['teamNumber'].map(lambda t: epas.get(int(t), {}).get('epa_total',   0.0))
        df['epa_auto']    = df['teamNumber'].map(lambda t: epas.get(int(t), {}).get('epa_auto',    0.0))
        df['epa_teleop']  = df['teamNumber'].map(lambda t: epas.get(int(t), {}).get('epa_teleop',  0.0))
        df['epa_endgame'] = df['teamNumber'].map(lambda t: epas.get(int(t), {}).get('epa_endgame', 0.0))

        # --- Tier Calculation ---
        def norm(series):
            mn, mx = series.min(), series.max()
            return (series - mn) / (mx - mn) if mx != mn else pd.Series([0.5]*len(series), index=series.index)

        # Volley success rate used if data available, else fall back to FER
        volley_data_available = df['avg_volleys_attempted'].sum() > 0
        auto_volley_data_available = df['avg_auto_volleys_attempted'].sum() > 0
        tele_scoring = norm(df['volley_quality_score']) if volley_data_available else norm(df['shift_efficiency'])
        auto_scoring = norm(df['auto_volley_quality_score']) if auto_volley_data_available else norm(df['shift_efficiency'])
        scoring_metric = (0.6 * tele_scoring + 0.4 * auto_scoring)

        scout_score = (
            0.45 * norm(df['avg_contributed_points']) +
            0.30 * scoring_metric +
            0.25 * norm(df['climb_reliability'])
        )
        epa_available = df['epa_total'].sum() > 0
        epa_score = norm(df['epa_total']) if epa_available else pd.Series([0.5]*len(df), index=df.index)

        df['combined_score'] = 0.6 * scout_score + 0.4 * epa_score if epa_available else scout_score

        p85 = df['combined_score'].quantile(0.85)
        p60 = df['combined_score'].quantile(0.60)
        p35 = df['combined_score'].quantile(0.35)
        p15 = df['combined_score'].quantile(0.15)

        def assign_tier(score):
            if score >= p85:   return '🏆 Elite'
            elif score >= p60: return '⬆️ High'
            elif score >= p35: return '➡️ Medium'
            elif score >= p15: return '⬇️ Low'
            else:              return '⛔ None'

        df['tier'] = df['combined_score'].apply(assign_tier)

    return df

@st.cache_data
def load_team_trend(team_number):
    """Loads per-match data for a specific team ordered by match number."""
    query = """
    SELECT
        matchNumber, contributedPoints,
        autoVolleysAttempted, autoVolleyQuality,
        volleysAttempted, volleyQuality,
        defendedTime, teleClimb, climbTime,
        startPosition, matchNotes
    FROM match_data
    WHERE teamNumber = ?
    ORDER BY matchNumber ASC
    """
    conn = sq.connect(DB_PATH)
    df = pd.read_sql_query(query, conn, params=(team_number,))
    conn.close()
    return df

@st.cache_data
def load_all_team_trends():
    """Loads per-match contributedPoints for ALL teams for momentum calculation."""
    query = """
    SELECT teamNumber, matchNumber, contributedPoints, teleFuel
    FROM match_data
    WHERE teamNumber IS NOT NULL
    ORDER BY teamNumber, matchNumber ASC
    """
    conn = sq.connect(DB_PATH)
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

@st.cache_data
def load_prematch_teams(teams):
    """Loads stats for specific team numbers for pre-match comparison."""
    placeholders = ", ".join("?" * len(teams))
    query = f"""
    SELECT
        m.teamNumber,
        COUNT(m.matchNumber)                        AS matches_played,
        AVG(m.contributedPoints)                    AS avg_contributed_points,
        AVG(m.autoFuel + m.teleFuel)                AS avg_total_fuel,
        ROUND(AVG(CAST(m.defendedTime AS REAL)), 3) AS defended_pct,
        ROUND(AVG(
            CASE m.autoVolleyQuality
                WHEN 'Perfect'       THEN 1.000
                WHEN 'Above Average' THEN 0.750
                WHEN 'Average'       THEN 0.500
                WHEN 'Below Average' THEN 0.125
                ELSE                      0.500
            END
        ), 3)                                       AS auto_volley_quality_score,
        ROUND(AVG(
            CASE m.volleyQuality
                WHEN 'Perfect'       THEN 1.000
                WHEN 'Above Average' THEN 0.750
                WHEN 'Average'       THEN 0.500
                WHEN 'Below Average' THEN 0.125
                ELSE                      0.500
            END
        ), 3)                                       AS volley_quality_score,
        ROUND(
            SUM(CASE WHEN m.teleClimb != 'None' AND m.teleClimb != 'N/A' AND m.teleClimb != '' THEN 1 ELSE 0 END)
            * 1.0 / COUNT(m.matchNumber), 3
        )                                           AS climb_reliability,
        p.driveBaseType, p.autoStartPref, p.roboStrat, p.climbAbility
    FROM match_data m
    LEFT JOIN pit_data p ON m.teamNumber = p.teamNumber
    WHERE m.teamNumber IN ({placeholders})
    GROUP BY m.teamNumber
    """
    conn = sq.connect(DB_PATH)
    df = pd.read_sql_query(query, conn, params=teams)
    conn.close()
    return df

@st.cache_data
def load_pit_data(team_number):
    query = "SELECT * FROM pit_data WHERE teamNumber = ?"
    conn = sq.connect(DB_PATH)
    df = pd.read_sql_query(query, conn, params=(team_number,))
    conn.close()
    return df

@st.cache_data
def load_auto_position_breakdown(team_number):
    query = """
    SELECT startPosition, COUNT(matchNumber) AS matches,
           AVG(autoFuel) AS avg_auto_fuel
    FROM match_data
    WHERE teamNumber = ?
    GROUP BY startPosition
    ORDER BY avg_auto_fuel DESC
    """
    conn = sq.connect(DB_PATH)
    df = pd.read_sql_query(query, conn, params=(team_number,))
    conn.close()
    return df

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================
def calculate_momentum(df_trend):
    if len(df_trend) < 2:
        return 0.0
    x = np.arange(len(df_trend))
    col = 'contributedPoints' if 'contributedPoints' in df_trend.columns else 'teleFuel'
    y = df_trend[col].fillna(0).values
    slope = np.polyfit(x, y, 1)[0]
    return round(slope, 4)

def momentum_arrow(slope):
    if slope > 0.05:   return "⬆️"
    elif slope < -0.05: return "⬇️"
    else:               return "➡️"

def color_tier_row(row):
    tier = row.get('Tier', '')
    if '🏆' in tier:   return ['background-color: rgba(255, 215, 0, 0.18)']  * len(row)  # Elite  — gold
    elif '⬆️' in tier: return ['background-color: rgba(0, 200, 100, 0.15)']  * len(row)  # High   — green
    elif '➡️' in tier: return ['background-color: rgba(100, 149, 237, 0.12)']* len(row)  # Medium — blue
    elif '⬇️' in tier: return ['background-color: rgba(220, 50, 50, 0.10)']  * len(row)  # Low    — red
    else:               return ['background-color: rgba(80, 80, 80, 0.08)']   * len(row)  # None   — grey

def norm_col(series):
    mn, mx = series.min(), series.max()
    return (series - mn) / (mx - mn) if mx != mn else pd.Series([0.5]*len(series), index=series.index)

# =============================================================================
# SIDEBAR
# =============================================================================
st.sidebar.title("E-TECH Chargers 2026")
st.sidebar.markdown("---")
_default_page = st.session_state.pop('page_override', "Match Center")
page = st.sidebar.radio(
    "Navigation",
    ["Match Center", "Pre-Match Predictor", "Picklist", "Team Deep-Dive", "Data Management"],
    index=["Match Center", "Pre-Match Predictor", "Picklist", "Team Deep-Dive", "Data Management"].index(_default_page)
)
st.sidebar.markdown("---")
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

if 'prematch_teams' not in st.session_state:
    st.session_state.prematch_teams = {'red': [None, None, None], 'blue': [None, None, None]}

# =============================================================================
# PAGE: PICKLIST
# =============================================================================
if page == "Picklist":
    st.title("🏆 Dynamic Pick List")
    st.markdown("Adjust sliders to weight what your alliance needs. The table re-ranks and color-codes by tier instantly.")

    st.subheader("Metric Weights")
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        w_prop = st.slider("Proportional Score", 0, 100, 25, step=1)
    with col2:
        w_fer = st.slider("Shift Efficiency", 0, 100, 15, step=1)
    with col3:
        w_volley = st.slider("Tele Volley Quality", 0, 100, 15, step=1)
    with col4:
        w_auto_volley = st.slider("Auto Volley Quality", 0, 100, 15, step=1)
    col5, col6, col7 = st.columns(3)
    with col5:
        w_consistency = st.slider("Consistency", 0, 100, 10, step=1)
    with col6:
        w_climb = st.slider("Climb Reliability", 0, 100, 10, step=1)
    with col7:
        w_epa = st.slider("EPA (Statbotics)", 0, 100, 10, step=1)

    total_weight = w_prop + w_fer + w_volley + w_auto_volley + w_consistency + w_climb + w_epa

    if total_weight == 0:
        st.warning("Weights must add up to more than 0.")
    else:
        df = load_team_averages()
        if df.empty:
            st.info("No match data found in the database yet.")
        else:
            # Exclude already-picked teams
            all_team_nums = sorted(df['teamNumber'].dropna().astype(int).tolist())
            excluded = st.multiselect(
                "🚫 Exclude already-picked teams",
                options=all_team_nums,
                placeholder="Select teams to hide from the list..."
            )
            if excluded:
                df = df[~df['teamNumber'].isin(excluded)]

            # Momentum
            all_trends = load_all_team_trends()
            momentum_map = {
                team: calculate_momentum(group.reset_index(drop=True))
                for team, group in all_trends.groupby('teamNumber')
            }
            df['momentum_slope'] = df['teamNumber'].map(momentum_map).fillna(0.0)

            # Consistency score
            if 'tele_fuel_stddev' in df.columns and df['tele_fuel_stddev'].max() > 0:
                df['consistency_score'] = 1 - norm_col(df['tele_fuel_stddev'])
            else:
                df['consistency_score'] = 0.5

            # Normalize and score
            volley_data = df['avg_volleys_attempted'].sum() > 0
            epa_data    = df['epa_total'].sum() > 0

            for col in ['avg_proportional_score', 'avg_contributed_points',
                        'shift_efficiency', 'volley_quality_score', 'auto_volley_quality_score',
                        'climb_reliability', 'epa_total']:
                df[f'{col}_norm'] = norm_col(df[col].fillna(0))

            # Fallbacks when data not yet available
            volley_data      = df['avg_volleys_attempted'].sum() > 0
            auto_volley_data = df['avg_auto_volleys_attempted'].sum() > 0
            volley_norm      = df['volley_quality_score_norm'] if volley_data else df['shift_efficiency_norm']
            auto_volley_norm = df['auto_volley_quality_score_norm'] if auto_volley_data else df['shift_efficiency_norm']

            prop_data = df['avg_proportional_score'].sum() > 0
            prop_norm = df['avg_proportional_score_norm'] if prop_data else df['avg_contributed_points_norm']

            df['score'] = (
                (w_prop        / total_weight) * prop_norm +
                (w_fer         / total_weight) * df['shift_efficiency_norm'] +
                (w_volley      / total_weight) * volley_norm +
                (w_auto_volley / total_weight) * auto_volley_norm +
                (w_consistency / total_weight) * df['consistency_score'] +
                (w_climb       / total_weight) * df['climb_reliability_norm'] +
                (w_epa         / total_weight) * df['epa_total_norm']
            )

            df = df.sort_values('score', ascending=False).reset_index(drop=True)
            df.index += 1

            display_df = df[[
                'teamNumber', 'tier', 'matches_played',
                'avg_proportional_score', 'avg_contributed_points', 'shift_efficiency',
                'avg_auto_volleys_attempted', 'auto_volley_quality_score',
                'avg_volleys_attempted', 'volley_quality_score',
                'climb_reliability', 'epa_total', 'score'
            ]].copy()

            display_df['Trend'] = df['momentum_slope'].apply(momentum_arrow)

            display_df = display_df.rename(columns={
                'teamNumber':              'Team',
                'tier':                   'Tier',
                'matches_played':         'Matches',
                'avg_proportional_score': 'Prop. Score',
                'avg_contributed_points': 'Avg Pts',
                'shift_efficiency':        'Shift Eff.',
                'avg_auto_volleys_attempted': 'Auto Volleys',
                'auto_volley_quality_score':  'Auto Volley Q.',
                'avg_volleys_attempted':      'Tele Volleys',
                'volley_quality_score':       'Tele Volley Q.',
                'climb_reliability':      'Climb %',
                'epa_total':              'EPA',
                'score':                  'Weighted Score'
            })

            st.markdown("---")
            st.subheader("Ranked Team List")
            st.caption("🏆 Elite  ⬆️ High  ➡️ Medium  ⬇️ Low  ⛔ None")

            if not prop_data:
                st.caption("⚠️ No proportional score yet — using Contributed Points as fallback.")
            if not volley_data:
                st.caption("⚠️ No tele volley data yet — Tele Volley Quality weight using Shift Efficiency as fallback.")
            if not epa_data:
                st.caption("⚠️ Statbotics EPA not yet available — tier based on scouting data only.")

            styled = (
                display_df.style
                .apply(color_tier_row, axis=1)
                .format({
                    'Prop. Score':  '{:.1f}',
                    'Avg Pts':      '{:.1f}',
                    'FER':          '{:.3f}',
                    'Volleys':      '{:.1f}',
                    'Auto Volley Q.': '{:.0%}',
                    'Tele Volley Q.': '{:.0%}',
                    'Climb %':      '{:.0%}',
                    'EPA':          '{:.1f}',
                    'Weighted Score': '{:.3f}'
                })
            )
            st.dataframe(styled, use_container_width=True)

# =============================================================================
# PAGE: PRE-MATCH PREDICTOR
# =============================================================================
elif page == "Pre-Match Predictor":
    st.title("🎯 Pre-Match Strategy Board")
    st.markdown("Enter the six teams for the upcoming match to compare their core metrics side-by-side.")

    pre = st.session_state.prematch_teams
    col_red, col_blue = st.columns(2)
    with col_red:
        st.markdown("### 🔴 Red Alliance")
        red1 = st.number_input("Red 1", min_value=1, max_value=99999, value=pre['red'][0], placeholder="Team #")
        red2 = st.number_input("Red 2", min_value=1, max_value=99999, value=pre['red'][1], placeholder="Team #")
        red3 = st.number_input("Red 3", min_value=1, max_value=99999, value=pre['red'][2], placeholder="Team #")
    with col_blue:
        st.markdown("### 🔵 Blue Alliance")
        blue1 = st.number_input("Blue 1", min_value=1, max_value=99999, value=pre['blue'][0], placeholder="Team #")
        blue2 = st.number_input("Blue 2", min_value=1, max_value=99999, value=pre['blue'][1], placeholder="Team #")
        blue3 = st.number_input("Blue 3", min_value=1, max_value=99999, value=pre['blue'][2], placeholder="Team #")

    teams = [int(t) for t in [red1, red2, red3, blue1, blue2, blue3] if t is not None]

    if len(teams) < 6:
        st.info("Enter all six team numbers to generate the strategy board.")
    else:
        df = load_prematch_teams(tuple(teams))
        if df.empty:
            st.warning("No data found for any of these teams.")
        else:
            red_teams = [int(t) for t in [red1, red2, red3] if t is not None]
            df['Alliance'] = df['teamNumber'].apply(
                lambda t: '🔴 Red' if t in red_teams else '🔵 Blue'
            )
            df = df.sort_values('Alliance')

            # Pull tier and EPA and merge in
            df_all = load_team_averages()
            if not df_all.empty:
                df = df.merge(
                    df_all[['teamNumber', 'tier', 'epa_total']],
                    on='teamNumber', how='left'
                )
            else:
                df['tier'] = 'No data'
                df['epa_total'] = 0.0

            # Fill nulls from LEFT JOIN
            df['autoStartPref']      = df['autoStartPref'].fillna('No pit data')
            df['robotStrategy']      = df['robotStrategy'].fillna('No pit data')
            df['climb_reliability']  = df['climb_reliability'].fillna(0.0)
            df['volley_quality_score']= df['volley_quality_score'].fillna(0.0)
            df['tier']               = df['tier'].fillna('No data')
            df['epa_total']          = df['epa_total'].fillna(0.0)

            st.markdown("---")
            st.subheader("Side-by-Side Comparison")
            st.dataframe(
                df[[
                    'Alliance', 'teamNumber', 'tier', 'matches_played',
                    'avg_contributed_points', 'volley_quality_score',
                    'climb_reliability', 'epa_total',
                    'autoStartPref', 'robotStrategy'
                ]].rename(columns={
                    'teamNumber':             'Team',
                    'tier':                  'Tier',
                    'matches_played':        'Matches',
                    'avg_contributed_points':'Avg Pts',
                    'volley_quality_score':  'Volley Quality',
                    'climb_reliability':     'Climb %',
                    'epa_total':             'EPA',
                    'autoStartPref':         'Auto Start Pref',
                    'robotStrategy':         'Strategy'
                }).style.format({
                    'Avg Pts':   '{:.1f}',
                    'Auto Volley Q.': '{:.0%}',
                    'Tele Volley Q.': '{:.0%}',
                    'Climb %':   '{:.0%}',
                    'EPA':       '{:.1f}'
                }),
                use_container_width=True
            )

            # Biggest scoring threat
            biggest_threat = df.loc[df['avg_contributed_points'].idxmax()]
            st.markdown("---")
            st.warning(
                f"⚠️ **Biggest Scoring Threat:** Team **{int(biggest_threat['teamNumber'])}** "
                f"({biggest_threat['Alliance']}) — Avg Pts: {biggest_threat['avg_contributed_points']:.1f}  |  "
                f"Tier: {biggest_threat['tier']}"
            )

            # Auto path conflict check
            st.markdown("---")
            st.subheader("🗺️ Auto Path Conflict Check")
            red_df  = df[df['Alliance'] == '🔴 Red'][['teamNumber', 'autoStartPref']]
            blue_df = df[df['Alliance'] == '🔵 Blue'][['teamNumber', 'autoStartPref']]
            col_r, col_b = st.columns(2)
            with col_r:
                st.markdown("**🔴 Red Auto Starts**")
                st.dataframe(red_df.rename(columns={'teamNumber': 'Team', 'autoStartPref': 'Preferred Start'}), use_container_width=True)
            with col_b:
                st.markdown("**🔵 Blue Auto Starts**")
                st.dataframe(blue_df.rename(columns={'teamNumber': 'Team', 'autoStartPref': 'Preferred Start'}), use_container_width=True)

# =============================================================================
# PAGE: TEAM DEEP-DIVE
# =============================================================================
elif page == "Team Deep-Dive":
    st.title("🔍 Team Deep-Dive")
    st.markdown("Full performance profile for any team in the database.")

    df_all = load_team_averages()
    if df_all.empty:
        st.info("No match data found in the database yet.")
    else:
        team_list = sorted(df_all['teamNumber'].dropna().astype(int).tolist())
        selected_team = st.selectbox("Select a Team", team_list)

        df_trend   = load_team_trend(selected_team)
        df_pit     = load_pit_data(selected_team)
        df_auto_pos= load_auto_position_breakdown(selected_team)
        summary    = df_all[df_all['teamNumber'] == selected_team].iloc[0]
        momentum_slope = calculate_momentum(df_trend)

        # --- Tier Badge ---
        st.markdown("---")
        tier = summary.get('tier', 'No data')
        epa  = summary.get('epa_total', 0.0)
        tier_colors = {
            '🏆 Elite':  'rgba(255, 215, 0, 0.25)',
            '⬆️ High':   'rgba(0, 200, 100, 0.20)',
            '➡️ Medium': 'rgba(100, 149, 237, 0.20)',
            '⬇️ Low':    'rgba(220, 50, 50, 0.15)',
            '⛔ None':   'rgba(80, 80, 80, 0.15)',
        }
        bg = tier_colors.get(tier, 'rgba(150,150,150,0.15)')
        st.markdown(
            f'<div style="background:{bg}; padding:10px 18px; border-radius:8px; '
            f'font-size:1.2em; font-weight:600; display:inline-block; margin-bottom:12px;">'
            f'Tier: {tier}</div>',
            unsafe_allow_html=True
        )

        # --- Summary Metrics ---
        st.subheader(f"Team {selected_team} — Summary")
        m1, m2, m3, m4, m5, m6, m7, m8 = st.columns(8)
        m1.metric("Matches Played",     int(summary['matches_played']))
        m2.metric("Prop. Score",        f"{summary.get('avg_proportional_score', 0):.1f}" if summary.get('avg_proportional_score', 0) > 0 else "No data")
        m3.metric("Avg Contributed Pts",f"{summary.get('avg_contributed_points', 0):.1f}")
        m4.metric("Shift Efficiency",   f"{summary.get('shift_efficiency', 0):.3f}")
        m5.metric("Auto Volley Q.",     f"{summary.get('auto_volley_quality_score', 0):.0%}" if summary.get('avg_auto_volleys_attempted', 0) > 0 else "No data")
        m6.metric("Tele Volley Q.",     f"{summary.get('volley_quality_score', 0):.0%}" if summary.get('avg_volleys_attempted', 0) > 0 else "No data")
        m7.metric("Climb Reliability",  f"{summary.get('climb_reliability', 0):.0%}")
        m8.metric("EPA (Statbotics)",   f"{epa:.1f}" if epa > 0 else "N/A")

        # --- Trend Chart ---
        st.markdown("---")
        st.subheader("📈 Performance Trend")
        chart_mode = st.radio("Chart Mode", ["Contributed Points", "Fuel (Auto vs Tele)", "Volleys"], horizontal=True)

        if not df_trend.empty:
            fig_df = df_trend.copy()
            fig_df['matchNumber'] = fig_df['matchNumber'].astype(str)

            if chart_mode == "Contributed Points":
                fig = px.line(fig_df, x='matchNumber', y='contributedPoints', markers=True,
                              title=f"Team {selected_team} — Contributed Points per Match",
                              labels={'matchNumber': 'Match', 'contributedPoints': 'Points'})
                # Add trend line
                if len(df_trend) >= 2:
                    x_vals = np.arange(len(df_trend))
                    y_fit  = np.polyval(np.polyfit(x_vals, df_trend['contributedPoints'].fillna(0).values, 1), x_vals)
                    fig.add_scatter(x=fig_df['matchNumber'], y=y_fit, mode='lines',
                                    name='Trend', line=dict(dash='dash', color='orange'))

            elif chart_mode == "Fuel (Auto vs Tele)":
                melted = fig_df.melt(id_vars='matchNumber', value_vars=['autoFuel', 'teleFuel'],
                                     var_name='Phase', value_name='Fuel')
                fig = px.line(melted, x='matchNumber', y='Fuel', color='Phase', markers=True,
                              title=f"Team {selected_team} — Fuel by Phase",
                              labels={'matchNumber': 'Match'})

            else:  # Volleys
                if 'volleysAttempted' in fig_df.columns and fig_df['volleysAttempted'].sum() > 0:
                    fig = px.bar(fig_df, x='matchNumber', y='volleysAttempted',
                                 title=f"Team {selected_team} — Volleys Attempted per Match",
                                 labels={'matchNumber': 'Match', 'volleysAttempted': 'Volleys'})
                    # Overlay quality as color
                    if 'volleyQuality' in fig_df.columns:
                        fig = px.bar(fig_df, x='matchNumber', y='volleysAttempted', color='volleyQuality',
                                     color_discrete_map={'Perfect': '#8e44ad', 'Above Average': '#2ecc71', 'Average': '#f39c12', 'Below Average': '#e74c3c'},
                                     title=f"Team {selected_team} — Volleys by Quality",
                                     labels={'matchNumber': 'Match', 'volleysAttempted': 'Volleys', 'volleyQuality': 'Quality'})
                else:
                    st.info("No volley data collected yet for this team.")
                    fig = None

            if fig:
                st.plotly_chart(fig, use_container_width=True)

        # --- Defense Impact ---
        st.markdown("---")
        st.subheader("🛡️ Defense Impact Analysis")
        if not df_trend.empty and 'defendedTime' in df_trend.columns:
            defended   = df_trend[df_trend['defendedTime'] == True]
            undefended = df_trend[df_trend['defendedTime'] == False]
            col_d1, col_d2, col_d3 = st.columns(3)
            col_d1.metric("Avg Pts When Defended",
                          f"{defended['contributedPoints'].mean():.1f}" if not defended.empty else "N/A")
            col_d2.metric("Avg Pts Undefended",
                          f"{undefended['contributedPoints'].mean():.1f}" if not undefended.empty else "N/A")
            if not defended.empty and not undefended.empty:
                impact = undefended['contributedPoints'].mean() - defended['contributedPoints'].mean()
                col_d3.metric("Defense Impact", f"{impact:+.1f} pts", delta_color="inverse")
        else:
            st.info("No defended time data available.")

        # --- Auto Position Breakdown ---
        st.markdown("---")
        st.subheader("🗺️ Auto Performance by Start Position")
        if not df_auto_pos.empty:
            df_auto_pos['avg_auto_fuel'] = df_auto_pos['avg_auto_fuel'].fillna(0.0)
            st.dataframe(
                df_auto_pos.rename(columns={
                    'startPosition': 'Start Position',
                    'matches':       'Matches',
                    'avg_auto_fuel': 'Avg Auto Fuel'
                }).style.format({'Avg Auto Fuel': '{:.1f}'}),
                use_container_width=True
            )
        else:
            st.info("No auto position data available.")

        # --- Pit Data ---
        st.markdown("---")
        st.subheader("🔧 Pit Scouting Data")
        if not df_pit.empty:
            pit = df_pit.iloc[0]
            p1, p2, p3 = st.columns(3)
            with p1:
                st.markdown("**Robot**")
                st.write(f"Drive Train: {pit.get('driveTrain', 'N/A')}")
                st.write(f"Dimensions: {pit.get('robotWidth', '?')}\" W × {pit.get('robotLength', '?')}\" L × {pit.get('robotHeight', '?')}\" H")
                st.write(f"Extends Frame: {pit.get('extendsFrame', 'N/A')} ({pit.get('extensionDir', 'N/A')})")
                st.write(f"Can Retract: {pit.get('canRetract', 'N/A')}")
            with p2:
                st.markdown("**Strategy**")
                st.write(f"Driver Experience: {pit.get('driverExp', 'N/A')}")
                st.write(f"Auto Start Pref: {pit.get('autoStartPref', 'N/A')}")
                st.write(f"Driver Station Pref: {pit.get('driverStationPref', 'N/A')}")
                st.write(f"Strategy: {pit.get('robotStrategy', 'N/A')}")
                st.write(f"Best Auto: {pit.get('bestAutoDescription', 'N/A')}")
            with p3:
                st.markdown("**Capabilities**")
                st.write(f"Climb Ability: {pit.get('climbAbility', 'N/A')}")
                st.write(f"Intake Source: {pit.get('intakeSource', 'N/A')}")
                st.write(f"Shot Mobility: {pit.get('shotMobility', 'N/A')}")
                st.write(f"Shot Range: {pit.get('shotRange', 'N/A')}")
            st.markdown("**Notes**")
            st.info(pit.get('pitNotes', 'No notes recorded.'))
        else:
            st.info(f"No pit scouting data found for team {selected_team}.")

        # --- Match Log ---
        st.markdown("---")
        st.subheader("📋 Full Match Log")
        if not df_trend.empty:
            st.dataframe(df_trend.rename(columns={
                'matchNumber':      'Match',
                'contributedPoints':'Points',
                'autoFuel':         'Auto Fuel',
                'teleFuel':         'Tele Fuel',
                'autoVolleysAttempted': 'Auto Volleys',
                'autoVolleyQuality':    'Auto Volley Q.',
                'volleysAttempted':     'Tele Volleys',
                'volleyQuality':        'Tele Volley Q.',
                'defendedTime':     'Defended?',
                'teleClimb':        'Climb',
                'climbTime':        'Climb Time',
                'startPosition':    'Start Pos',
                'matchNotes':       'Notes'
            }), use_container_width=True)

# =============================================================================
# PAGE: DATA MANAGEMENT
# =============================================================================
elif page == "Data Management":
    st.title("📂 Data Management")
    st.markdown("Upload Scoutradioz CSV exports to update the database.")

    tab_match, tab_pit = st.tabs(["📊 Match Data", "🔧 Pit Data"])

    with tab_match:
        st.subheader("Upload Match Data CSV")
        uploaded_match = st.file_uploader("Choose a match data CSV", type="csv", key="match_uploader")
        if uploaded_match is not None:
            preview_df = pd.read_csv(uploaded_match)
            uploaded_match.seek(0)
            st.markdown("---")
            st.subheader("Preview — First 5 Rows")
            st.dataframe(preview_df.head(), use_container_width=True)
            st.caption(f"{len(preview_df)} rows detected across {len(preview_df.columns)} columns.")
            st.markdown("---")
            st.subheader("Confirm Upload")
            st.warning("⚠️ This will write match data into the database. Existing entries for the same match/team combinations will be overwritten.")
            confirmation = st.radio("Are you sure you want to upload this match data?",
                                    options=["No", "Yes"], index=0, key="match_confirm")
            if confirmation == "Yes":
                if st.button("✅ Confirm and Upload Match Data"):
                    with st.spinner("Writing to database..."):
                        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
                            tmp.write(uploaded_match.getvalue())
                            tmp_path = Path(tmp.name)
                        try:
                            process_match_data(tmp_path, DB_PATH)
                            st.cache_data.clear()
                            st.success(f"✅ Successfully loaded {len(preview_df)} rows into 'match_data'!")
                        except Exception as e:
                            st.error(f"❌ Something went wrong: {e}")
                        finally:
                            tmp_path.unlink(missing_ok=True)
            else:
                st.info("Select **Yes** above and then click Confirm to proceed.")

    with tab_pit:
        st.subheader("Upload Pit Data CSV")
        uploaded_pit = st.file_uploader("Choose a pit data CSV", type="csv", key="pit_uploader")
        if uploaded_pit is not None:
            preview_pit = pd.read_csv(uploaded_pit)
            uploaded_pit.seek(0)
            st.markdown("---")
            st.subheader("Preview — First 5 Rows")
            st.dataframe(preview_pit.head(), use_container_width=True)
            st.caption(f"{len(preview_pit)} rows detected across {len(preview_pit.columns)} columns.")
            st.markdown("---")
            st.subheader("Confirm Upload")
            st.warning("⚠️ This will write pit data into the database. Existing entries for the same team will be overwritten.")
            pit_confirmation = st.radio("Are you sure you want to upload this pit data?",
                                        options=["No", "Yes"], index=0, key="pit_confirm")
            if pit_confirmation == "Yes":
                if st.button("✅ Confirm and Upload Pit Data"):
                    with st.spinner("Writing to database..."):
                        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
                            tmp.write(uploaded_pit.getvalue())
                            tmp_path = Path(tmp.name)
                        try:
                            process_pit_data(tmp_path, DB_PATH)
                            st.cache_data.clear()
                            st.success(f"✅ Successfully loaded {len(preview_pit)} rows into 'pit_data'!")
                        except Exception as e:
                            st.error(f"❌ Something went wrong: {e}")
                        finally:
                            tmp_path.unlink(missing_ok=True)
            else:
                st.info("Select **Yes** above and then click Confirm to proceed.")

# =============================================================================
# PAGE: MATCH CENTER
# =============================================================================
elif page == "Match Center":
    st.title("📡 Match Center")
    st.markdown(f"Live event data for **Team {OUR_TEAM}** at event `{EVENT_KEY}`.")
    st.caption("Data refreshes every 60 seconds. Hit 🔄 Refresh Data in the sidebar to force update.")

    match_status,   our_matches  = get_our_matches()
    ranking_status, our_ranking  = get_team_ranking()
    _,              all_rankings = get_event_rankings()

    if match_status == API_ERROR:
        st.error(api_error_message(our_matches if isinstance(our_matches, str) else "no_connection"))
        st.stop()
    elif match_status == API_NO_DATA and ranking_status == API_NO_DATA:
        st.info(
            f"📭 No event data available yet for **{EVENT_KEY}** on The Blue Alliance.\n\n"
            "This is normal before the event begins. Match schedules and rankings are usually "
            "posted the morning of the first day of competition. Check back then!"
        )
        st.stop()

    # Rankings summary
    st.markdown("---")
    st.subheader(f"📊 Team {OUR_TEAM} — Event Standing")
    if our_ranking:
        r = our_ranking
        total_teams = len(all_rankings)
        record = r.get('record', {})
        wins, losses, ties = record.get('wins', 0), record.get('losses', 0), record.get('ties', 0)
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Rank",           f"{r.get('rank', '?')} / {total_teams}")
        c2.metric("Record",         f"{wins}W - {losses}L - {ties}T")
        c3.metric("Ranking Score",  f"{r.get('ranking_score', 0):.2f}")
        c4.metric("Matches Played", wins + losses + ties)
        c5.metric("Matches Left",   len([m for m in our_matches if not parse_match(m)['played']]))
    else:
        st.info("Rankings not yet available.")

    # Match schedule
    st.markdown("---")
    st.subheader(f"📅 Match Schedule — Team {OUR_TEAM}")
    if our_matches:
        for m in our_matches:
            p = parse_match(m)
            alliance_color = "🔴" if p['our_alliance'] == 'Red' else "🔵"
            with st.expander(
                f"Match {p['match_number']}  |  {alliance_color} {p['our_alliance']}  |  "
                f"{p['result']}  |  {p['our_score']} – {p['opp_score']}",
                expanded=not p['played']
            ):
                col_a, col_b, col_c = st.columns(3)
                with col_a:
                    st.markdown("**Alliance Partners**")
                    st.write(", ".join(str(t) for t in p['partners']))
                with col_b:
                    st.markdown("**Opponents**")
                    st.write(", ".join(str(t) for t in p['opponents']))
                with col_c:
                    if not p['played']:
                        if st.button(f"🎯 Scout Match {p['match_number']} in Pre-Match Predictor",
                                     key=f"scout_{p['match_number']}"):
                            st.session_state.prematch_teams = {
                                'red': p['red_teams'], 'blue': p['blue_teams']
                            }
                            st.session_state['page_override'] = "Pre-Match Predictor"
                            st.rerun()
                    else:
                        if p['result'] == '✅ Win':    st.success("Result: Win")
                        elif p['result'] == '❌ Loss': st.error("Result: Loss")
                        else:                          st.info("Result: Tie")
    else:
        st.info(f"No matches found for Team {OUR_TEAM} at event {EVENT_KEY}.")

    # Full event rankings
    st.markdown("---")
    st.subheader("🏆 Full Event Rankings")
    if all_rankings:
        rows = []
        for r in all_rankings:
            record = r.get('record', {})
            rows.append({
                'Rank': r.get('rank'), 'Team': int(r['team_key'][3:]),
                'W': record.get('wins', 0), 'L': record.get('losses', 0), 'T': record.get('ties', 0),
                'Ranking Score': r.get('ranking_score', 0),
            })
        rankings_df = pd.DataFrame(rows)

        def highlight_our_team(row):
            if row['Team'] == OUR_TEAM:
                return ['background-color: rgba(0, 150, 255, 0.20)'] * len(row)
            return [''] * len(row)

        st.dataframe(
            rankings_df.style.apply(highlight_our_team, axis=1).format({'Ranking Score': '{:.2f}'}),
            use_container_width=True, hide_index=True
        )
    else:
        st.info("Rankings not yet available.")
