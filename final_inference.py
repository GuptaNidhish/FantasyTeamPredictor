import numpy as np 
import pandas as pd 
import joblib
from difflib import get_close_matches
from datetime import datetime, timezone
from db.initialization import SessionLocal
from db.models import PlayerMatchStats, Match
from sqlalchemy import func, case
from db.computefeatures import get_latest_players_df,compute_chunk1,compute_chunk2,compute_chunk3,compute_chunk4,compute_features
import requests

# ✅ CREATE SINGLE SESSION
session = SessionLocal()

data = pd.read_csv("/Users/nidhishgupta/Desktop/Dream11_Fantasy Team_Predictor/data/processed/full_req_data_final_final.csv")
historical_data = pd.read_csv("/Users/nidhishgupta/Desktop/Dream11_Fantasy Team_Predictor/data/processed/historical_data_final_final.csv")
model = joblib.load("/Users/nidhishgupta/Desktop/Dream11_Fantasy Team_Predictor/models/point_predicter_final.pkl")

API_KEY = "cb8a5495-0125-4122-9cef-e0993d41c40f"
SERIES_ID = "87c62aac-bc3c-4738-ab93-19da0690488f"

url = f"https://api.cricapi.com/v1/series_squad?apikey={API_KEY}&id={SERIES_ID}"
response = requests.get(url)
data_api_1 = response.json()

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
    
    return {
        "team1": next_match['teams'][0],
        "team2": next_match['teams'][1],
        "venue": next_match['venue'],
        "date": next_match['dateTimeGMT']
    }

def get_team_squads(data, team1, team2):
    team1_squad = []
    team2_squad = []

    for team in data['data']:
        team_name = team['teamName']

        if team_name.lower() == team1.lower():
            team1_squad = [player['name'] for player in team['players']]

        elif team_name.lower() == team2.lower():
            team2_squad = [player['name'] for player in team['players']]

    return team1_squad, team2_squad

url = f"https://api.cricapi.com/v1/series_info?apikey={API_KEY}&id={SERIES_ID}"
response = requests.get(url)
data_api_2 = response.json()

next_match = get_next_match(data_api_2)
print(next_match)

team1 = next_match['team1']
team2 = next_match['team2']
venue = next_match['venue']

print(venue)

team1_squad,team2_squad = get_team_squads(data_api_1,team1,team2)

print("Team 1 Squad:", team1_squad)
print("Team 2 Squad:", team2_squad)

squad = team1_squad+team2_squad

# ✅ REUSE SESSION
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

# ✅ REUSE SESSION
unique_venues = session.query(
    Match.venue
).distinct().all()

unique_venues = [p[0] for p in unique_venues]

def map_venue_name(name):
    match = get_close_matches(name, unique_venues, n=1, cutoff=0.7)
    if match:
        return match[0]
    else:
        return None

mapped_venue = map_venue_name(venue)

players_df = get_latest_players_df(all_mapped)
players_df = compute_features(players_df,team1,team2,mapped_venue)
players_df['venue'] = mapped_venue

def get_pitch_type_for_venue(session, venue):
    result = session.query(Match.pitch_type)\
        .filter(Match.venue == venue)\
        .distinct()\
        .all()
    
    pitch_types = [r[0] for r in result if r[0] is not None]
    
    return pitch_types[0] if pitch_types else None

# ✅ REUSE SESSION
mapped_pitch_type = get_pitch_type_for_venue(session,mapped_venue)

players_df['pitch_type'] = mapped_pitch_type

# ✅ REUSE SESSION
teams_1 = session.query(Match.team1).distinct().all()
teams_2 = session.query(Match.team2).distinct().all()

unique_teams = list(set([t[0] for t in teams_1] + [t[0] for t in teams_2]))

def map_team_name(name, unique_teams):
    match = get_close_matches(name, unique_teams, n=1, cutoff=0.85)
    return match[0] if match else None

mapped_team1 = map_team_name(team1,unique_teams)
mapped_team2 = map_team_name(team2,unique_teams)

player_team_map = {}

for p in mapped_players_1:
    player_team_map[p] = mapped_team1

for p in mapped_players_2:
    player_team_map[p] = mapped_team2

players_df['team'] = players_df['player'].map(player_team_map)

players_df['opponent'] = players_df['team'].apply(
    lambda x: mapped_team2 if x == mapped_team1 else mapped_team1
)

players_df_encoded = pd.get_dummies(players_df, columns=['team', 'opponent', 'pitch_type'])

model_columns = joblib.load("/Users/nidhishgupta/Desktop/Dream11_Fantasy Team_Predictor/models/model_cols.pkl")

for col in model_columns:
    if col not in players_df_encoded.columns:
        players_df_encoded[col] = 0

players_df_encoded = players_df_encoded[model_columns]

# ✅ REUSE SESSION
mean_sr = session.query(
    func.avg(
        (PlayerMatchStats.runs / func.nullif(PlayerMatchStats.balls_played, 0)) * 100
    )
).scalar()

print("Global Mean SR:", mean_sr)

players_df['rolling_strike_rate'] = pd.to_numeric(
    players_df['rolling_strike_rate'], errors='coerce'
)

players_df['rolling_strike_rate'] = players_df['rolling_strike_rate'].fillna(mean_sr)

players_df_encoded = players_df_encoded.apply(pd.to_numeric, errors='coerce')
players_df_encoded = players_df_encoded.fillna(0)

players_df['predicted_points'] = model.predict(players_df_encoded)

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

# ✅ REUSE SESSION
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

).group_by(
    PlayerMatchStats.player_name
).all()

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

print("🏏 Final Team:")
print(team)

print("\n📊 Role Distribution:")
print(role_count)

print("\n🏢 Team Distribution:")
print(team_count)

print("\n👑 Captain:", captain)
print("🤝 Vice-Captain:", vice_captain)

print(players_df[['player','predicted_points']])

# ✅ CLOSE SESSION AT VERY END
session.close()