import streamlit as st
import requests
import time
import json
import os

from db.initialization import SessionLocal
from ingest_New_match_data import ingest_match_data
from run_inference import run_inference_pipeline

# ================= CONFIG =================
API_KEY = "cb8a5495-0125-4122-9cef-e0993d41c40f"
SERIES_ID = "87c62aac-bc3c-4738-ab93-19da0690488f"

PROCESSED_MATCH_FILE = "processed_matches.json"

st.set_page_config(
    page_title="Dream11 Predictor",
    page_icon="🏏",
    layout="wide"
)

# ================= LOAD/SAVE =================
def load_processed_matches():
    if not os.path.exists(PROCESSED_MATCH_FILE):
        return set()
    with open(PROCESSED_MATCH_FILE, "r") as f:
        return set(json.load(f))

def save_processed_matches(processed_matches):
    with open(PROCESSED_MATCH_FILE, "w") as f:
        json.dump(list(processed_matches), f, indent=2)

# ================= API =================
def fetch_series_info():
    url = f"https://api.cricapi.com/v1/series_info?apikey={API_KEY}&id={SERIES_ID}"
    return requests.get(url).json()

def fetch_match_scorecard(match_id):
    url = f"https://api.cricapi.com/v1/match_scorecard?apikey={API_KEY}&id={match_id}"
    return requests.get(url).json()

def fetch_series_squad():
    url = f"https://api.cricapi.com/v1/series_squad?apikey={API_KEY}&id={SERIES_ID}"
    return requests.get(url).json()

# ================= SQUAD =================
def get_team_squads(data, team1, team2):
    team1_squad, team2_squad = [], []

    for team in data['data']:
        if team['teamName'].lower() == team1.lower():
            team1_squad = [p['name'] for p in team['players']]

        elif team['teamName'].lower() == team2.lower():
            team2_squad = [p['name'] for p in team['players']]

    return team1_squad, team2_squad

# ================= INGESTION =================
def run_ingestion():
    logs = []
    processed = load_processed_matches()

    try:
        data = fetch_series_info()

        if 'data' not in data:
            logs.append("❌ Invalid series info")
            return logs

        matches = data['data']['matchList']

        for match in matches:
            if not match.get('matchEnded', False):
                continue

            match_id = match['id']

            if match_id in processed:
                logs.append("⚠️ Match already processed")
                return logs

            team1, team2 = match['teams']
            logs.append(f"✅ Found match: {team1} vs {team2}")

            scorecard = fetch_match_scorecard(match_id)
            squad_data = fetch_series_squad()

            if 'data' not in scorecard or scorecard['data'] is None:
                logs.append("❌ Invalid scorecard")
                return logs

            if 'data' not in squad_data or squad_data['data'] is None:
                logs.append("❌ Invalid squad data")
                return logs

            teams = scorecard['data'].get('teams', [])
            if len(teams) != 2:
                logs.append("❌ Invalid team data")
                return logs

            team1_squad, team2_squad = get_team_squads(
                squad_data, team1, team2
            )

            if not team1_squad or not team2_squad:
                logs.append("❌ Squad mapping failed")
                return logs

            session = SessionLocal()

            try:
                ingest_match_data(
                    api_response=scorecard,
                    session=session,
                    team1_squad=team1_squad,
                    team2_squad=team2_squad
                )

                session.commit()

                processed.add(match_id)
                save_processed_matches(processed)

                logs.append("✅ Ingestion successful")

            except Exception as e:
                session.rollback()
                logs.append(f"❌ DB Error: {str(e)}")

            finally:
                session.close()

            return logs

        logs.append("⚠️ No completed matches found")

    except Exception as e:
        logs.append(f"❌ Error: {str(e)}")

    return logs

# ================= UI =================
st.title("🏏 Dream11 Fantasy Team Predictor")

st.sidebar.header("⚙️ Controls")

run_prediction = st.sidebar.button("🔮 Run Prediction")
run_ingest = st.sidebar.button("📥 Run Ingestion")

# ================= PREDICTION =================
if run_prediction:
    st.subheader("🔮 Predicted Team")

    with st.spinner("Running AI model..."):
        result = run_inference_pipeline()

    if result:
        team = result['team']
        captain = result['captain']
        vice_captain = result['vice_captain']

        st.success("✅ Prediction Ready!")

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("### 👑 Captain")
            st.info(captain)

        with col2:
            st.markdown("### 🤝 Vice Captain")
            st.info(vice_captain)

        st.markdown("### 🏏 Playing XI")

        for player in team:
            if player == captain:
                st.write(f"👑 {player}")
            elif player == vice_captain:
                st.write(f"🤝 {player}")
            else:
                st.write(f"• {player}")

    else:
        st.error("❌ Prediction failed")

# ================= INGESTION =================
if run_ingest:
    st.subheader("📥 Data Ingestion")

    with st.spinner("Running ingestion pipeline..."):
        logs = run_ingestion()

    st.success("✅ Process Finished")

    st.markdown("### 📜 Logs")

    for log in logs:
        if "❌" in log:
            st.error(log)
        elif "⚠️" in log:
            st.warning(log)
        else:
            st.info(log)