def ingest_latest_completed_match():
    import requests
    from db.initialization import SessionLocal
    from ingest_New_match_data import ingest_match_data
    from db.models import NextMatch, ProcessedMatch

    API_KEY = "cb8a5495-0125-4122-9cef-e0993d41c40f"
    SERIES_ID = "87c62aac-bc3c-4738-ab93-19da0690488f"

    logs = []

    session = SessionLocal()

    # ================= HELPERS =================
    def load_processed_matches():
        """
        ✅ CHANGED: Use match_id instead of id
        """
        processed = session.query(ProcessedMatch.match_id).all()  # ✅ FIXED
        return set([p[0] for p in processed])

    def save_processed_match(match_id):
        """
        ✅ CHANGED: Use match_id column
        """
        exists = session.query(ProcessedMatch).filter(
            ProcessedMatch.match_id == match_id  # ✅ FIXED
        ).first()

        if exists:
            return

        new_entry = ProcessedMatch(match_id=match_id)  # ✅ FIXED
        session.add(new_entry)
        session.commit()

    try:
        # ================= LOAD MATCH QUEUE =================
        matches_queue = session.query(NextMatch).all()

        if not matches_queue:
            return ["❌ Match queue empty (no matches in DB)"]

        processed = load_processed_matches()

        # ================= PROCESS ALL UNPROCESSED MATCHES =================
        for next_match in matches_queue:
            match_id = next_match.id  # (this stays same, NextMatch uses id)

            if match_id in processed:
                logs.append(f"⚠️ Match {match_id} already processed. Skipping.")
                continue

            team1 = next_match.team1
            team2 = next_match.team2

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
            ingest_session = SessionLocal()
            try:
                ingest_match_data(
                    api_response=scorecard,
                    session=ingest_session,
                    team1_squad=team1_squad,
                    team2_squad=team2_squad
                )
                ingest_session.commit()
                logs.append(f"✅ Ingestion successful for match {match_id}")

                # ================= SAVE PROCESSED MATCH =================
                save_processed_match(match_id)

            except Exception as e:
                ingest_session.rollback()
                logs.append(f"❌ DB Error for match {match_id}: {str(e)}")

            finally:
                ingest_session.close()

    except Exception as e:
        logs.append(f"❌ Error: {str(e)}")

    finally:
        session.close()

    return logs