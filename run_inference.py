def run_inference_pipeline():
    import numpy as np 
    import pandas as pd 
    import joblib
    from difflib import get_close_matches
    from datetime import datetime, timezone
    from db.initialization import SessionLocal
    from db.models import PlayerMatchStats, Match
    from sqlalchemy import func, case
    from db.computefeatures import get_latest_players_df, compute_features
    import requests
    import json
    import os
    import pickle
    # ================= FILE TO STORE ALL UPCOMING MATCHES =================
    NEXT_MATCH_FILE = "next_match.json"
    MODEL_COLS_URL = "https://mpyitncpkyunkqccefit.supabase.co/storage/v1/object/public/Models/model_cols.pkl"
    MODEL_COLS_PATH = "modelcols.pkl"
    MODEL_URL = "https://mpyitncpkyunkqccefit.supabase.co/storage/v1/object/public/Models/point_predicter_final.pkl"
    MODEL_PATH = "model.pkl"
    def download_model_cols():
        print("Downloading model cols from Supabase...")
        response = requests.get(MODEL_COLS_URL)
        if response.status_code == 200:
            with open(MODEL_COLS_PATH, "wb") as f:
                f.write(response.content)
            print("Model cols downloaded successfully!")
        else:
            raise Exception("Failed to download model cols")
    def download_model():
        print("Downloading model from Supabase...")
        response = requests.get(MODEL_URL)
        if response.status_code == 200:
            with open(MODEL_PATH, "wb") as f:
                f.write(response.content)
            print("Model downloaded successfully!")
        else:
            raise Exception("Failed to download model")
    # ================= FIXED: ROBUST SAVE FUNCTION =================
    def save_next_match(match):
        """
        Save the next upcoming match to next_match.json.
        Appends to file if it exists and avoids duplicates.
        """

        # ✅ FIX: Handle existing file safely (list/dict/corrupt cases)
        if os.path.exists(NEXT_MATCH_FILE):
            with open(NEXT_MATCH_FILE, "r") as f:
                try:
                    existing = json.load(f)

                    # ✅ FIX: Convert old dict format → list
                    if isinstance(existing, dict):
                        existing = [existing]

                    # ✅ FIX: If somehow not list → reset
                    if not isinstance(existing, list):
                        existing = []

                except json.JSONDecodeError:
                    existing = []
        else:
            existing = []

        # ✅ FIX: Safe duplicate check (avoid crash if bad data present)
        if not any(isinstance(m, dict) and m.get('id') == match['id'] for m in existing):
            existing.append(match)

        # Save updated list
        with open(NEXT_MATCH_FILE, "w") as f:
            json.dump(existing, f, indent=2)

    # ================= CREATE SINGLE SESSION =================
    session = SessionLocal()

    if not os.path.exists(MODEL_PATH):
        download_model()

    # Now load model

    with open(MODEL_PATH, "rb") as f:
        model = pickle.load(f)

    API_KEY = "fcc8ef0d-6e5c-462f-822c-d1bab2031cc6"
    SERIES_ID = "87c62aac-bc3c-4738-ab93-19da0690488f"

    # ================= API CALLS =================
    url = f"https://api.cricapi.com/v1/series_squad?apikey={API_KEY}&id={SERIES_ID}"
    response = requests.get(url)
    data_api_1 = response.json()

    url = f"https://api.cricapi.com/v1/series_info?apikey={API_KEY}&id={SERIES_ID}"
    response = requests.get(url)
    data_api_2 = response.json()

    # ================= HELPERS =================
    def get_next_match(series_info_json):
        match_list = series_info_json['data']['matchList']
        now = datetime.now(timezone.utc)
        upcoming_matches = []
        
        for match in match_list:
            match_time = datetime.fromisoformat(match['dateTimeGMT']).replace(tzinfo=timezone.utc)
            if match_time > now and not match['matchStarted']:
                upcoming_matches.append(match)
        
        if not upcoming_matches:
            return None
        
        upcoming_matches.sort(
            key=lambda x: datetime.fromisoformat(x['dateTimeGMT']).replace(tzinfo=timezone.utc)
        )
        
        next_match = upcoming_matches[0]
        img1 = ""
        img2 = ""
        if(next_match['teamInfo'][0]['name'] == next_match['teams'][0]):
            img1 = next_match['teamInfo'][0]['img']
            img2 = next_match['teamInfo'][1]['img']
        elif(next_match['teamInfo'][0]['name'] == next_match['teams'][1]):
            img2 = next_match['teamInfo'][0]['img']
            img1 = next_match['teamInfo'][1]['img']
        return {
            "id": next_match['id'],
            "team1": next_match['teams'][0],
            "team2": next_match['teams'][1],
            "venue": next_match['venue'],
            "date": next_match['dateTimeGMT'],
            "img1": img1,
            "img2": img2
        }

    def get_team_squads(data, team1, team2):
        team1_squad = []
        team2_squad = []
        playerttoimg = {}

        for team in data['data']:
            team_name = team['teamName'].lower()

            if team_name == team1.lower():
                for player in team['players']:
                    name = player['name']
                    team1_squad.append(name)
                    playerttoimg[name] = player.get('playerImg')  # safe access

            elif team_name == team2.lower():
                for player in team['players']:
                    name = player['name']
                    team2_squad.append(name)
                    playerttoimg[name] = player.get('playerImg')  # safe access

        return team1_squad, team2_squad, playerttoimg

    next_match = get_next_match(data_api_2)

    if not next_match:
        print("❌ No upcoming matches found")
        session.close()
        return None

    # ================= SAVE MATCH (APPEND + NO DUPLICATES) =================
    save_next_match(next_match)

    team1 = next_match['team1']
    team2 = next_match['team2']
    venue = next_match['venue']

    team1_squad, team2_squad,playertoimg = get_team_squads(data_api_1, team1, team2)

    # ================= PLAYER MAPPING =================
    unique_players = session.query(
        PlayerMatchStats.player_name
    ).distinct().all()
    unique_players = [p[0] for p in unique_players]

    def map_player_name(name, unique_players):
        match = get_close_matches(name, unique_players, n=1, cutoff=0.85)
        return match[0] if match else None

    mapped_players_1 = [
        m for p in team1_squad 
        if (m := map_player_name(p, unique_players)) is not None
    ]

    mapped_players_2 = [
        m for p in team2_squad 
        if (m := map_player_name(p, unique_players)) is not None
    ]

    all_mapped = mapped_players_1 + mapped_players_2

    # ================= VENUE =================
    unique_venues = session.query(Match.venue).distinct().all()
    unique_venues = [p[0] for p in unique_venues]

    def map_venue_name(name):
        match = get_close_matches(name, unique_venues, n=1, cutoff=0.7)
        return match[0] if match else None

    mapped_venue = map_venue_name(venue)

    # ================= FEATURE ENGINEERING =================
    players_df = get_latest_players_df(all_mapped)
    players_df = compute_features(players_df, team1, team2, mapped_venue)
    players_df['venue'] = mapped_venue

    def get_pitch_type_for_venue(session, venue):
        result = session.query(Match.pitch_type)\
            .filter(Match.venue == venue)\
            .distinct()\
            .all()
        
        pitch_types = [r[0] for r in result if r[0] is not None]
        return pitch_types[0] if pitch_types else None

    mapped_pitch_type = get_pitch_type_for_venue(session, mapped_venue)
    players_df['pitch_type'] = mapped_pitch_type

    # ================= TEAM MAPPING =================
    teams_1 = session.query(Match.team1).distinct().all()
    teams_2 = session.query(Match.team2).distinct().all()
    unique_teams = list(set([t[0] for t in teams_1] + [t[0] for t in teams_2]))

    def map_team_name(name, unique_teams):
        match = get_close_matches(name, unique_teams, n=1, cutoff=0.85)
        return match[0] if match else None

    mapped_team1 = map_team_name(team1, unique_teams)
    mapped_team2 = map_team_name(team2, unique_teams)

    player_team_map = {}
    for p in mapped_players_1:
        player_team_map[p] = mapped_team1
    for p in mapped_players_2:
        player_team_map[p] = mapped_team2

    players_df['team'] = players_df['player'].map(player_team_map)
    players_df['opponent'] = players_df['team'].apply(
        lambda x: mapped_team2 if x == mapped_team1 else mapped_team1
    )

    # ================= ENCODING =================
    players_df_encoded = pd.get_dummies(players_df, columns=['team', 'opponent', 'pitch_type'])
    if not os.path.exists(MODEL_COLS_PATH):
        download_model_cols()

    # Now load model cols

    with open(MODEL_COLS_PATH, "rb") as f:
        model_columns = pickle.load(f)
    for col in model_columns:
        if col not in players_df_encoded.columns:
            players_df_encoded[col] = 0

    players_df_encoded = players_df_encoded[model_columns]

    # ================= HANDLE NaNs =================
    mean_sr = session.query(
        func.avg(
            (PlayerMatchStats.runs / func.nullif(PlayerMatchStats.balls_played, 0)) * 100
        )
    ).scalar()

    players_df['rolling_strike_rate'] = pd.to_numeric(
        players_df['rolling_strike_rate'], errors='coerce'
    )

    players_df['rolling_strike_rate'] = players_df['rolling_strike_rate'].fillna(mean_sr)

    players_df_encoded = players_df_encoded.apply(pd.to_numeric, errors='coerce')
    players_df_encoded = players_df_encoded.fillna(0)

    # ================= PREDICTION =================
    players_df['predicted_points'] = model.predict(players_df_encoded)

    # ================= ROLE CLASSIFICATION =================
    batting_points = (
        PlayerMatchStats.runs +
        PlayerMatchStats.fours +
        PlayerMatchStats.sixes
    )

    bowling_points = (
        PlayerMatchStats.wickets * 25
    )

    denominator = func.nullif(
        PlayerMatchStats.fantasy_points - PlayerMatchStats.fielding_points,
        0
    )

    player_roles = session.query(
        PlayerMatchStats.player_name.label("player"),
        case(
            (
                (func.avg(batting_points / denominator) > 0.15) &
                (func.avg(bowling_points / denominator) > 0.15),
                'AR'
            ),
            (
                func.avg(batting_points / denominator) >=
                func.avg(bowling_points / denominator),
                'BAT'
            ),
            else_='BOWL'
        ).label("role")
    ).group_by(PlayerMatchStats.player_name).all()

    role_map = {player: role for player, role in player_roles}
    players_df['role'] = players_df['player'].map(role_map).fillna('BAT')

    players_df = players_df.sort_values(
        by='predicted_points',
        ascending=False
    ).reset_index(drop=True)

    def get_role(row):
        if row['player_role_wicketkeeper']:
            return 'WK'
        return row['role'].upper()

    players_df['final_role'] = players_df.apply(get_role, axis=1)

    # ================= TEAM SELECTION =================
    def build_valid_team(players_df):
        team = []
        team_count = {}
        
        role_constraints = {
            'WK': (1, 4),
            'BAT': (3, 6),
            'AR': (1, 4),
            'BOWL': (3, 6)
        }

        role_count = {r: 0 for r in role_constraints}

        for role, (min_req, max_req) in role_constraints.items():
            candidates = players_df[
                (players_df['final_role'] == role) &
                (~players_df['player'].isin(team))
            ]
            
            for _, row in candidates.iterrows():
                if role_count[role] >= min_req:
                    break
                    
                team_name = row['team']
                
                if team_count.get(team_name, 0) >= 7:
                    continue
                
                team.append(row['player'])
                role_count[role] += 1
                team_count[team_name] = team_count.get(team_name, 0) + 1

        for _, row in players_df.iterrows():
            if len(team) >= 11:
                break
                
            player = row['player']
            role = row['final_role']
            team_name = row['team']
            
            if player in team:
                continue
            
            if team_count.get(team_name, 0) >= 7:
                continue
            
            if role_count[role] >= role_constraints[role][1]:
                continue
            
            team.append(player)
            role_count[role] += 1
            team_count[team_name] = team_count.get(team_name, 0) + 1

        return team, role_count, team_count

    def choose_captains(team, players_df):
        selected = players_df[players_df['player'].isin(team)]
        selected = selected.sort_values(by='predicted_points', ascending=False)
        
        captain = selected.iloc[0]['player']
        vice_captain = selected.iloc[1]['player']
        
        return captain, vice_captain

    team, role_count, team_count = build_valid_team(players_df)
    captain, vice_captain = choose_captains(team, players_df)

    print("🏏 Final Team:", team)
    print("\n📊 Role Distribution:", role_count)
    print("\n🏢 Team Distribution:", team_count)
    print("\n👑 Captain:", captain)
    print("🤝 Vice-Captain:", vice_captain)

    session.close()

    return {
        "team": team,
        "captain": captain,
        "vice_captain": vice_captain,
        "players_df": players_df,
        "team1_name":next_match['team1'],
        "team2_name":next_match['team2'],
        "team1_img": next_match['img1'],
        "team2_img": next_match['img2'],
        "playertoimg":playertoimg
    }