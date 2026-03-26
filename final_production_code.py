import schedule
import time
import requests
import json
import os

from db.initialization import SessionLocal
from ingest_New_match_data import ingest_match_data
from run_inference import run_inference_pipeline

API_KEY = "cb8a5495-0125-4122-9cef-e0993d41c40f"
SERIES_ID = "87c62aac-bc3c-4738-ab93-19da0690488f"

PROCESSED_MATCH_FILE = "processed_matches.json"

# ================= LOAD/SAVE =================
def load_processed_matches():
    if not os.path.exists(PROCESSED_MATCH_FILE):
        return set()
    with open(PROCESSED_MATCH_FILE, "r") as f:
        return set(json.load(f))

def save_processed_matches(processed_matches):
    with open(PROCESSED_MATCH_FILE, "w") as f:
        json.dump(list(processed_matches), f)

# ================= API CALLS =================
def fetch_series_info():
    url = f"https://api.cricapi.com/v1/series_info?apikey={API_KEY}&id={SERIES_ID}"
    return requests.get(url).json()

def fetch_match_scorecard(match_id):
    # ✅ CORRECT: using match_scorecard endpoint for ingestion
    url = f"https://api.cricapi.com/v1/match_scorecard?apikey={API_KEY}&id={match_id}"
    return requests.get(url).json()

def fetch_series_squad():
    # ✅ CHANGED: using series_squad (as you requested, not match_squad)
    url = f"https://api.cricapi.com/v1/series_squad?apikey={API_KEY}&id={SERIES_ID}"
    return requests.get(url).json()

# ================= SQUAD EXTRACTION =================
def get_team_squads(data, team1, team2):
    team1_squad = []
    team2_squad = []

    for team in data['data']:
        if team['teamName'].lower() == team1.lower():
            team1_squad = [p['name'] for p in team['players']]

        elif team['teamName'].lower() == team2.lower():
            team2_squad = [p['name'] for p in team['players']]

    return team1_squad, team2_squad

# ================= INGESTION =================
def run_new_matches_ingestion(match_id):
    print(f"\n📥 Starting ingestion for match: {match_id}")

    session = SessionLocal()

    try:
        scorecard = fetch_match_scorecard(match_id)

        # ✅ CHANGED: using series_squad instead of match_squad
        squad_data = fetch_series_squad()

        # ✅ SAFETY: check if API returned valid data
        if 'data' not in scorecard or scorecard['data'] is None:
            print("❌ Invalid scorecard response")
            return

        team1, team2 = scorecard['data']['teams']

        team1_squad, team2_squad = get_team_squads(squad_data, team1, team2)

        ingest_match_data(
            api_response=scorecard,
            session=session,
            team1_squad=team1_squad,
            team2_squad=team2_squad
        )

        session.commit()

    except Exception as e:
        session.rollback()
        print("❌ Ingestion failed:", str(e))

    finally:
        session.close()

# ================= CHECK & INGEST =================
def check_and_ingest():
    print("\n🕒 Checking for completed matches...")

    processed = load_processed_matches()

    try:
        data = fetch_series_info()
        matches = data['data']['matchList']

        for match in matches:
            match_id = match['id']  # ✅ API match ID (correct)

            if match_id in processed:
                continue

            if match.get('matchEnded', False):
                team1, team2 = match['teams']

                print(f"\n✅ Match Ended: {team1} vs {team2}")

                run_new_matches_ingestion(match_id)

                processed.add(match_id)
                save_processed_matches(processed)

                print("✅ Stored as processed")

    except Exception as e:
        print("❌ Error:", str(e))

# ================= INFERENCE =================
def run_prediction_cycle():
    print("\n🔮 Running Prediction...")

    try:
        result = run_inference_pipeline()

        if result:
            print("\n🏏 Team:", result['team'])
            print("👑 Captain:", result['captain'])
            print("🤝 Vice-Captain:", result['vice_captain'])

    except Exception as e:
        print("❌ Prediction error:", str(e))

# ================= SCHEDULER =================
def run_scheduler():
    print("🚀 Dream11 Automation Started...\n")

    # 🟡 Prediction before match
    schedule.every().day.at("14:00").do(run_prediction_cycle)

    # 🟢 Match ingestion timings
    schedule.every().day.at("18:30").do(check_and_ingest)
    schedule.every().day.at("23:30").do(check_and_ingest)

    # 🔴 Safety fallback
    schedule.every().day.at("02:00").do(check_and_ingest)

    # 🔁 Extra safety
    schedule.every(2).hours.do(check_and_ingest)

    while True:
        schedule.run_pending()
        time.sleep(30)

# ================= RUN =================
if __name__ == "__main__":
    run_scheduler()