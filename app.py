import tempfile
import numpy as np
import pandas as pd
import sqlite3 as sq
import streamlit as st
import plotly.express as px
from pathlib import Path
from data_processor import process_match_data, process_pit_data

# --- CONFIG ---
st.set_page_config(page_title="E-TECH CHARGERS SCOUTING DASHBOARD", layout="wide")

DB_PATH = Path(__file__).resolve().parent / 'database' / 'scouting_2026.db'

# --- DATA LOADERS ---
@st.cache_data
def load_team_averages():
    """Loads per-team aggregated stats including consistency, climb, momentum, and alliance contribution."""
    query = """
    SELECT
        teamNumber,
        COUNT(matchNumber)                          AS matches_played,
        AVG(auto_bps)                               AS avg_auto_bps,
        AVG(tele_bps)                               AS avg_tele_bps,
        AVG(tele_bps * tele_bps)                    AS avg_tele_bps_sq,
        AVG(autoFuel + teleFuel)                    AS avg_total_fuel,
        AVG(teleAccuracy)                           AS avg_accuracy,
        AVG(defendedTime)                           AS avg_defended_time,
        AVG(contributedPoints)                      AS avg_contributed_points,

        -- Climb reliability: ratio of non-zero climb entries
        ROUND(
            SUM(CASE WHEN teleClimb != 'None' AND teleClimb != 'N/A' AND teleClimb != '' THEN 1 ELSE 0 END)
            * 1.0 / COUNT(matchNumber), 3
        )                                           AS climb_reliability
    FROM match_data
    GROUP BY teamNumber
    """
    conn = sq.connect(DB_PATH)
    df = pd.read_sql_query(query, conn)
    conn.close()

    if not df.empty:
        df['avg_tele_bps'] = df['avg_tele_bps'].fillna(0.0)
        df['avg_tele_bps_sq'] = df['avg_tele_bps_sq'].fillna(0.0)
        variance = (df['avg_tele_bps_sq'] - (df['avg_tele_bps'] ** 2)).clip(lower=0.0)
        df['tele_bps_stddev'] = np.sqrt(variance)
        df = df.drop(columns=['avg_tele_bps_sq'])
    return df

@st.cache_data
def load_team_trend(team_number):
    """Loads per-match BPS data for a specific team ordered by match number."""
    query = """
    SELECT
        matchNumber,
        auto_bps,
        tele_bps,
        autoFuel,
        teleFuel,
        teleAccuracy,
        defendedTime,
        teleClimb,
        climbTime,
        startPosition,
        matchNotes
    FROM match_data
    WHERE teamNumber = ?
    ORDER BY matchNumber ASC
    """
    conn = sq.connect(DB_PATH)
    df = pd.read_sql_query(query, conn, params=(team_number,))
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
        AVG(m.auto_bps)                             AS avg_auto_bps,
        AVG(m.tele_bps)                             AS avg_tele_bps,
        AVG(m.autoFuel + m.teleFuel)                AS avg_total_fuel,
        AVG(m.teleAccuracy)                         AS avg_accuracy,
        AVG(m.defendedTime)                         AS avg_defended_time,
        ROUND(
            SUM(CASE WHEN m.teleClimb != 'None' AND m.teleClimb != 'N/A' AND m.teleClimb != '' THEN 1 ELSE 0 END)
            * 1.0 / COUNT(m.matchNumber), 3
        )                                           AS climb_reliability,
        p.driveTrain,
        p.autoStartPref,
        p.robotStrategy,
        p.climbAbility
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
    """Loads pit scouting data for a specific team."""
    query = "SELECT * FROM pit_data WHERE teamNumber = ?"
    conn = sq.connect(DB_PATH)
    df = pd.read_sql_query(query, conn, params=(team_number,))
    conn.close()
    return df

@st.cache_data
def load_auto_position_breakdown(team_number):
    """Loads auto fuel scored grouped by starting position."""
    query = """
    SELECT
        startPosition,
        COUNT(matchNumber)  AS matches,
        AVG(autoFuel)       AS avg_auto_fuel,
        AVG(auto_bps)       AS avg_auto_bps
    FROM match_data
    WHERE teamNumber = ?
    GROUP BY startPosition
    ORDER BY avg_auto_bps DESC
    """
    conn = sq.connect(DB_PATH)
    df = pd.read_sql_query(query, conn, params=(team_number,))
    conn.close()
    return df

def calculate_momentum(df_trend):
    """
    Fits a linear regression slope to tele_bps over match number.
    Returns the slope (positive = improving, negative = declining).
    """
    if len(df_trend) < 2:
        return 0.0
    x = np.arange(len(df_trend))
    y = df_trend['tele_bps'].values
    slope = np.polyfit(x, y, 1)[0]
    return round(slope, 4)

def momentum_arrow(slope):
    if slope > 0.01:
        return "⬆️"
    elif slope < -0.01:
        return "⬇️"
    else:
        return "➡️"

def color_momentum_row(row):
    """Returns a list of background colors for a dataframe row based on momentum slope."""
    slope = row.get('Momentum (Slope)', 0)
    if slope > 0.01:
        color = 'background-color: rgba(0, 200, 100, 0.15)'
    elif slope < -0.01:
        color = 'background-color: rgba(220, 50, 50, 0.15)'
    else:
        color = 'background-color: rgba(200, 200, 0, 0.10)'
    return [color] * len(row)

# --- SIDEBAR ---
st.sidebar.title("E-TECH Chargers 2026")
st.sidebar.markdown("---")
page = st.sidebar.radio("Navigation", ["Pre-Match Predictor", "Picklist", "Team Deep-Dive", "Data Management"])
st.sidebar.markdown("---")
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

# =============================================================================
# PAGE: PICKLIST
# =============================================================================
if page == "Picklist":
    st.title("🏆 Dynamic Pick List")
    st.markdown("Adjust sliders to weight what your alliance needs. The table re-ranks and color-codes instantly.")

    # --- Weight Sliders ---
    st.subheader("Metric Weights")
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    with col1:
        w_auto = st.slider("Auto BPS", 0, 100, 20, step=1)
    with col2:
        w_tele = st.slider("Tele BPS", 0, 100, 20, step=1)
    with col3:
        w_fuel = st.slider("Total Fuel", 0, 100, 20, step=1)
    with col4:
        w_consistency = st.slider("Consistency", 0, 100, 20, step=1)
    with col5:
        w_climb = st.slider("Climb Reliability", 0, 100, 10, step=1)
    with col6:
        w_momentum = st.slider("Momentum", 0, 100, 10, step=1)

    total_weight = w_auto + w_tele + w_fuel + w_consistency + w_climb + w_momentum

    if total_weight == 0:
        st.warning("Weights must add up to more than 0.")
    else:
        df = load_team_averages()

        if df.empty:
            st.info("No match data found in the database yet.")
        else:
            # Calculate momentum slope per team
            all_teams = df['teamNumber'].tolist()
            momentum_map = {}
            for team in all_teams:
                trend = load_team_trend(team)
                momentum_map[team] = calculate_momentum(trend)
            df['momentum_slope'] = df['teamNumber'].map(momentum_map)

            # Normalize metrics to 0-1 scale
            # Consistency: invert stddev so lower stddev = higher score
            df['consistency_score'] = 1 - (
                df['tele_bps_stddev'] / df['tele_bps_stddev'].max()
                if df['tele_bps_stddev'].max() > 0 else 0
            )

            for col in ['avg_auto_bps', 'avg_tele_bps', 'avg_total_fuel', 'climb_reliability']:
                col_max = df[col].max()
                df[f'{col}_norm'] = df[col] / col_max if col_max > 0 else 0.0

            # Normalize momentum: shift to 0-1 range
            m_min = df['momentum_slope'].min()
            m_max = df['momentum_slope'].max()
            df['momentum_norm'] = (
                (df['momentum_slope'] - m_min) / (m_max - m_min)
                if m_max != m_min else 0.5
            )

            # Calculate weighted score
            df['score'] = (
                (w_auto        / total_weight) * df['avg_auto_bps_norm'] +
                (w_tele        / total_weight) * df['avg_tele_bps_norm'] +
                (w_fuel        / total_weight) * df['avg_total_fuel_norm'] +
                (w_consistency / total_weight) * df['consistency_score'] +
                (w_climb       / total_weight) * df['climb_reliability_norm'] +
                (w_momentum    / total_weight) * df['momentum_norm']
            )

            df = df.sort_values('score', ascending=False).reset_index(drop=True)
            df.index += 1

            # Build display dataframe
            display_df = df[[
                'teamNumber', 'matches_played',
                'avg_auto_bps', 'avg_tele_bps', 'avg_total_fuel',
                'consistency_score', 'climb_reliability',
                'momentum_slope', 'score'
            ]].copy()

            display_df['momentum_arrow'] = display_df['momentum_slope'].apply(momentum_arrow)

            display_df = display_df.rename(columns={
                'teamNumber':        'Team',
                'matches_played':    'Matches',
                'avg_auto_bps':      'Avg Auto BPS',
                'avg_tele_bps':      'Avg Tele BPS',
                'avg_total_fuel':    'Avg Fuel',
                'consistency_score': 'Consistency',
                'climb_reliability': 'Climb %',
                'momentum_slope':    'Momentum (Slope)',
                'momentum_arrow':    'Trend',
                'score':             'Weighted Score'
            })

            st.markdown("---")
            st.subheader("Ranked Team List")
            st.caption("🟢 Improving  🔴 Declining  🟡 Stable")

            styled = (
                display_df.style
                .apply(color_momentum_row, axis=1)
                .format({
                    'Avg Auto BPS':      '{:.2f}',
                    'Avg Tele BPS':      '{:.2f}',
                    'Avg Fuel':          '{:.1f}',
                    'Consistency':       '{:.2f}',
                    'Climb %':           '{:.0f}%',
                    'Momentum (Slope)':  '{:.4f}',
                    'Weighted Score':    '{:.3f}'
                })
            )
            st.dataframe(styled, use_container_width=True)

# =============================================================================
# PAGE: PRE-MATCH PREDICTOR
# =============================================================================
elif page == "Pre-Match Predictor":
    st.title("🎯 Pre-Match Strategy Board")
    st.markdown("Enter the six teams for the upcoming match to compare their core metrics side-by-side.")

    col_red, col_blue = st.columns(2)
    with col_red:
        st.markdown("### 🔴 Red Alliance")
        red1 = st.number_input("Red 1", min_value=1, max_value=99999, value=None, placeholder="Team #")
        red2 = st.number_input("Red 2", min_value=1, max_value=99999, value=None, placeholder="Team #")
        red3 = st.number_input("Red 3", min_value=1, max_value=99999, value=None, placeholder="Team #")
    with col_blue:
        st.markdown("### 🔵 Blue Alliance")
        blue1 = st.number_input("Blue 1", min_value=1, max_value=99999, value=None, placeholder="Team #")
        blue2 = st.number_input("Blue 2", min_value=1, max_value=99999, value=None, placeholder="Team #")
        blue3 = st.number_input("Blue 3", min_value=1, max_value=99999, value=None, placeholder="Team #")

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

            st.markdown("---")
            st.subheader("Side-by-Side Comparison")
            st.dataframe(
                df[[
                    'Alliance', 'teamNumber', 'matches_played',
                    'avg_auto_bps', 'avg_tele_bps', 'avg_total_fuel',
                    'avg_accuracy', 'climb_reliability',
                    'autoStartPref', 'robotStrategy'
                ]].rename(columns={
                    'teamNumber':       'Team',
                    'matches_played':   'Matches',
                    'avg_auto_bps':     'Auto BPS',
                    'avg_tele_bps':     'Tele BPS',
                    'avg_total_fuel':   'Avg Fuel',
                    'avg_accuracy':     'Accuracy',
                    'climb_reliability':'Climb %',
                    'autoStartPref':    'Auto Start Pref',
                    'robotStrategy':    'Strategy'
                }).style.format({
                    'Auto BPS':  '{:.2f}',
                    'Tele BPS':  '{:.2f}',
                    'Avg Fuel':  '{:.1f}',
                    'Accuracy':  '{:.1f}%',
                    'Climb %':   '{:.0f}%'
                }),
                use_container_width=True
            )

            # Biggest scoring threat
            biggest_threat = df.loc[df['avg_tele_bps'].idxmax()]
            st.markdown("---")
            st.warning(
                f"⚠️ **Biggest Scoring Threat:** Team **{int(biggest_threat['teamNumber'])}** "
                f"({biggest_threat['Alliance']}) — Tele BPS: {biggest_threat['avg_tele_bps']:.2f}"
            )

            # Auto start position conflict check
            st.markdown("---")
            st.subheader("🗺️ Auto Path Conflict Check")
            red_df = df[df['Alliance'] == '🔴 Red'][['teamNumber', 'autoStartPref']]
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
        team_list = sorted(df_all['teamNumber'].astype(int).tolist())
        selected_team = st.selectbox("Select a Team", team_list)

        df_trend = load_team_trend(selected_team)
        df_pit = load_pit_data(selected_team)
        df_auto_pos = load_auto_position_breakdown(selected_team)
        summary = df_all[df_all['teamNumber'] == selected_team].iloc[0]
        momentum_slope = calculate_momentum(df_trend)

        # --- Summary Metrics ---
        st.markdown("---")
        st.subheader(f"Team {selected_team} — Summary")
        m1, m2, m3, m4, m5, m6 = st.columns(6)
        m1.metric("Matches Played",  int(summary['matches_played']))
        m2.metric("Avg Auto BPS",    f"{summary['avg_auto_bps']:.2f}")
        m3.metric("Avg Tele BPS",    f"{summary['avg_tele_bps']:.2f}")
        m4.metric("Avg Total Fuel",  f"{summary['avg_total_fuel']:.1f}")
        m5.metric("Climb Reliability", f"{summary['climb_reliability'] * 100:.0f}%")
        m6.metric("Momentum", f"{momentum_slope:+.4f} {momentum_arrow(momentum_slope)}")

        # --- BPS Trend Chart ---
        st.markdown("---")
        st.subheader("BPS Trend")
        chart_mode = st.radio("Chart Mode", ["Both", "Auto BPS Only", "Tele BPS Only"], horizontal=True)

        if not df_trend.empty:
            fig_df = df_trend.copy()
            fig_df['matchNumber'] = fig_df['matchNumber'].astype(str)

            if chart_mode == "Both":
                melted = fig_df.melt(
                    id_vars='matchNumber',
                    value_vars=['auto_bps', 'tele_bps'],
                    var_name='Phase', value_name='BPS'
                )
                fig = px.line(melted, x='matchNumber', y='BPS', color='Phase',
                              markers=True, title=f"Team {selected_team} — BPS Trend",
                              labels={'matchNumber': 'Match'})
            elif chart_mode == "Auto BPS Only":
                fig = px.line(fig_df, x='matchNumber', y='auto_bps', markers=True,
                              title=f"Team {selected_team} — Auto BPS Trend",
                              labels={'matchNumber': 'Match', 'auto_bps': 'Auto BPS'})
            else:
                fig = px.line(fig_df, x='matchNumber', y='tele_bps', markers=True,
                              title=f"Team {selected_team} — Tele BPS Trend",
                              labels={'matchNumber': 'Match', 'tele_bps': 'Tele BPS'})

            # Add momentum trend line to tele_bps when relevant
            if chart_mode in ["Both", "Tele BPS Only"] and len(df_trend) >= 2:
                x_vals = np.arange(len(df_trend))
                y_fit = np.polyval(np.polyfit(x_vals, df_trend['tele_bps'].values, 1), x_vals)
                trend_df = pd.DataFrame({
                    'matchNumber': fig_df['matchNumber'].values,
                    'Trend Line':  y_fit
                })
                fig.add_scatter(x=trend_df['matchNumber'], y=trend_df['Trend Line'],
                                mode='lines', name='Tele Trend',
                                line=dict(dash='dash', color='orange'))

            st.plotly_chart(fig, use_container_width=True)

        # --- Defense Impact Analysis ---
        st.markdown("---")
        st.subheader("🛡️ Defense Impact Analysis")
        if not df_trend.empty and 'defendedTime' in df_trend.columns:
            defended = df_trend[df_trend['defendedTime'] > 0]
            undefended = df_trend[df_trend['defendedTime'] == 0]
            col_d1, col_d2, col_d3 = st.columns(3)
            col_d1.metric("Avg BPS When Defended",
                          f"{defended['tele_bps'].mean():.2f}" if not defended.empty else "N/A")
            col_d2.metric("Avg BPS Undefended",
                          f"{undefended['tele_bps'].mean():.2f}" if not undefended.empty else "N/A")
            if not defended.empty and not undefended.empty:
                impact = undefended['tele_bps'].mean() - defended['tele_bps'].mean()
                col_d3.metric("Defense Impact", f"{impact:+.2f} BPS",
                              delta_color="inverse")
        else:
            st.info("No defended time data available.")

        # --- Auto Position Breakdown ---
        st.markdown("---")
        st.subheader("🗺️ Auto Performance by Start Position")
        if not df_auto_pos.empty:
            st.dataframe(
                df_auto_pos.rename(columns={
                    'startPosition': 'Start Position',
                    'matches':       'Matches',
                    'avg_auto_fuel': 'Avg Auto Fuel',
                    'avg_auto_bps':  'Avg Auto BPS'
                }).style.format({
                    'Avg Auto Fuel': '{:.1f}',
                    'Avg Auto BPS':  '{:.2f}'
                }),
                use_container_width=True
            )
        else:
            st.info("No auto position data available.")

        # --- Pit Data Panel ---
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
                'matchNumber':  'Match',
                'auto_bps':     'Auto BPS',
                'tele_bps':     'Tele BPS',
                'autoFuel':     'Auto Fuel',
                'teleFuel':     'Tele Fuel',
                'teleAccuracy': 'Accuracy',
                'defendedTime': 'Defended (s)',
                'teleClimb':    'Climb',
                'climbTime':    'Climb Time',
                'startPosition':'Start Pos',
                'matchNotes':   'Notes'
            }), use_container_width=True)

# =============================================================================
# PAGE: DATA MANAGEMENT
# =============================================================================
elif page == "Data Management":
    st.title("📂 Data Management")
    st.markdown("Upload Scoutradioz CSV exports to update the database.")

    tab_match, tab_pit = st.tabs(["📊 Match Data", "🔧 Pit Data"])

    # --- Match Data Upload ---
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

            confirmation = st.radio(
                "Are you sure you want to upload this match data?",
                options=["No", "Yes"],
                index=0,
                key="match_confirm"
            )

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

    # --- Pit Data Upload ---
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

            pit_confirmation = st.radio(
                "Are you sure you want to upload this pit data?",
                options=["No", "Yes"],
                index=0,
                key="pit_confirm"
            )

            if pit_confirmation == "Yes":
                if st.button("Confirm and Upload Pit Data"):
                    with st.spinner("Writing to database..."):
                        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
                            tmp.write(uploaded_pit.getvalue())
                            tmp_path = Path(tmp.name)
                        try:
                            process_pit_data(tmp_path, DB_PATH)
                            st.cache_data.clear()
                            st.success(f"Successfully loaded {len(preview_pit)} rows into 'pit_data'!")
                        except Exception as e:
                            st.error(f"Something went wrong: {e}")
                        finally:
                            tmp_path.unlink(missing_ok=True)
            else:
                st.info("Select **Yes** above and then click Confirm to proceed.")
