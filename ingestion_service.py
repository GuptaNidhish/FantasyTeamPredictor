def ingest_latest_completed_match():
    import requests
    import json
    import os
    from db.initialization import SessionLocal
    from ingest_New_match_data import ingest_match_data

    API_KEY = "fcc8ef0d-6e5c-462f-822c-d1bab2031cc6"
    SERIES_ID = "87c62aac-bc3c-4738-ab93-19da0690488f"
    NEXT_MATCH_FILE = "next_match.json"
    PROCESSED_MATCH_FILE = "processed_matches.json"  # ✅ Added file to track processed matches

    logs = []

    # ================= HELPERS =================
    def load_processed_matches():
        if not os.path.exists(PROCESSED_MATCH_FILE):
            return set()
        with open(PROCESSED_MATCH_FILE, "r") as f:
            return set(json.load(f))

    def save_processed_matches(processed_matches):
        with open(PROCESSED_MATCH_FILE, "w") as f:
            json.dump(list(processed_matches), f)

    try:
        # ================= LOAD MATCH QUEUE =================
        if not os.path.exists(NEXT_MATCH_FILE):
            return ["❌ No next match found (run prediction first)"]

        with open(NEXT_MATCH_FILE, "r") as f:
            try:
                matches_queue = json.load(f)
            except json.JSONDecodeError:
                matches_queue = []

        if not matches_queue:
            return ["❌ Match queue empty"]

        processed = load_processed_matches()

        # ================= PROCESS ALL UNPROCESSED MATCHES =================
        for next_match in matches_queue:
            match_id = next_match['id']

            # ✅ Skip already processed matches
            if match_id in processed:
                logs.append(f"⚠️ Match {match_id} already processed. Skipping.")
                continue

            team1 = next_match['team1']
            team2 = next_match['team2']
            logs.append(f"🎯 Target match: {team1} vs {team2}")

            # ================= FETCH SERIES INFO =================
            url = f"https://api.cricapi.com/v1/series_info?apikey={API_KEY}&id={SERIES_ID}"
            data = requests.get(url).json()

            if 'data' not in data:
                logs.append("❌ Invalid series info")
                continue

            matches = data['data']['matchList']

            # ================= FIND MATCH =================
            match_status = None
            for m in matches:
                if m['id'] == match_id:
                    match_status = m
                    break

            if not match_status:
                logs.append(f"❌ Match {match_id} not found in API")
                continue

            if not match_status.get('matchEnded', False):
                logs.append(f"⏳ Match {match_id} not finished yet")
                continue

            logs.append(f"✅ Match {match_id} ended. Starting ingestion...")

            # ================= FETCH SCORECARD =================
            scorecard_url = f"https://api.cricapi.com/v1/match_scorecard?apikey={API_KEY}&id={match_id}"
            scorecard = requests.get(scorecard_url).json()

            # ================= FETCH SQUADS =================
            squad_url = f"https://api.cricapi.com/v1/series_squad?apikey={API_KEY}&id={SERIES_ID}"
            squad_data = requests.get(squad_url).json()

            if 'data' not in scorecard or scorecard['data'] is None:
                logs.append(f"❌ Invalid scorecard for match {match_id}")
                continue

            if 'data' not in squad_data or squad_data['data'] is None:
                logs.append(f"❌ Invalid squad data for match {match_id}")
                continue

            # ================= GET SQUADS =================
            team1_squad, team2_squad = [], []
            for team in squad_data['data']:
                if team['teamName'].lower() == team1.lower():
                    team1_squad = [p['name'] for p in team['players']]
                elif team['teamName'].lower() == team2.lower():
                    team2_squad = [p['name'] for p in team['players']]

            if not team1_squad or not team2_squad:
                logs.append(f"❌ Squad mapping failed for match {match_id}")
                continue

            # ================= DB INGEST =================
            session = SessionLocal()
            try:
                ingest_match_data(
                    api_response=scorecard,
                    session=session,
                    team1_squad=team1_squad,
                    team2_squad=team2_squad
                )
                session.commit()
                logs.append(f"✅ Ingestion successful for match {match_id}")

                # ✅ Save match as processed
                processed.add(match_id)
                save_processed_matches(processed)

            except Exception as e:
                session.rollback()
                logs.append(f"❌ DB Error for match {match_id}: {str(e)}")

            finally:
                session.close()

    except Exception as e:
        logs.append(f"❌ Error: {str(e)}")

    return logs