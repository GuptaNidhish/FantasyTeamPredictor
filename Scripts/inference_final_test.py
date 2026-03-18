import numpy as np 
import pandas as pd 
import joblib
from difflib import get_close_matches
data = pd.read_csv("/Users/nidhishgupta/Desktop/Dream11_Fantasy Team_Predictor/data/processed/full_req_data_final.csv")
historical_data = pd.read_csv("/Users/nidhishgupta/Desktop/Dream11_Fantasy Team_Predictor/data/processed/historical_data_final.csv")
model = joblib.load("/Users/nidhishgupta/Desktop/Dream11_Fantasy Team_Predictor/models/point_predicter.pkl")
team1 = "Chennai Super Kings"
team2 = "Mumbai Indians"
venue = "Wankhede Stadium"
team1_squad =  [
    'MS Dhoni','Ravindra Jadeja','Ruturaj Gaikwad','Deepak Chahar','Moeen Ali',
'Shivam Dube','Matheesha Pathirana','Ajinkya Rahane','Daryl Mitchell',
'Mitchell Santner','Tushar Deshpande'
]
team2_squad = ['Rohit Sharma','Ishan Kishan','Suryakumar Yadav','Hardik Pandya',
'Tim David','Jasprit Bumrah','Piyush Chawla','Tilak Varma',
'Gerald Coetzee','Romario Shepherd','Nehal Wadhera'
]
squad = team1_squad+team2_squad
unique_player_list_from_df = historical_data['player'].unique()
unique_players = list(unique_player_list_from_df)
def map_player_name(name):
    
    match = get_close_matches(name, unique_players, n=1, cutoff=0.5)
    
    if match:
        return match[0]
    else:
        return None
mapped_players_1 = [map_player_name(p) for p in team1_squad]
mapped_players_2 = [map_player_name(p) for p in team2_squad]
all_mapped = mapped_players_1 + mapped_players_2
unique_venue_list_from_df = historical_data['venue'].unique()
unique_venues = list(unique_venue_list_from_df)
def map_venue_name(name):
    match = get_close_matches(name, unique_venues, n=1, cutoff=0.5)
    if match:
        return match[0]
    else:
        return None
mapped_venue = map_venue_name(venue)
players_df = data[
    data['player'].isin(all_mapped)
].copy()
players_df = (
    players_df
    .sort_values('player_match_number')
    .groupby('player')
    .tail(1)
)
players_df['venue'] = mapped_venue
players_df = players_df.drop(columns = ['venue_avg_points','venue_run_factor'])
venue_data = data[data['venue'] == mapped_venue][['player','venue_avg_points','venue_run_factor']].drop_duplicates()
players_df = players_df.merge(venue_data,on = ['player'],how = 'left')
players_df = players_df.drop(columns = ['venue_form'])
players_df['venue_form'] = players_df['venue_avg_points'] * players_df['recent_form']
ratio_info = historical_data[['player','player_match_number','last3_battingcontri','last3_bowlingcontri','last3_boundarypercentage']]
ratio_df = (
    ratio_info
    .sort_values('player_match_number')
    .groupby('player')
    .tail(1)
)
ratio_df = ratio_df.drop(columns = ['player_match_number'])
players_df = players_df.merge(ratio_df,on = ['player'],how = 'left')
players_df = players_df.drop(columns = ['batting_contribution_ratio','bowling_contribution_ratio','boundary_percentage'])
players_df = players_df.rename(columns = {"last3_battingcontri":"batting_contribution_ratio","last3_bowlingcontri":"bowling_contribution_ratio",'last3_boundarypercentage':'boundary_percentage'})
team_cols = [col for col in players_df.columns if col.startswith('team_')]
opponent_cols = [col for col in players_df.columns if col.startswith('opponent_')]
team_cols = team_cols[1:] # Removing team_won_toss from list
opponent_cols = opponent_cols[1:] # Removing opponent_avg_points from list
players_df[team_cols] = False
players_df[opponent_cols] = False
if f"team_{team1}" in team_cols:
    players_df.loc[players_df['player'].isin(mapped_players_1), f"team_{team1}"] = True

if f"team_{team2}" in team_cols:
    players_df.loc[players_df['player'].isin(mapped_players_2), f"team_{team2}"] = True
if f"opponent_{team2}" in opponent_cols:
    players_df.loc[players_df['player'].isin(mapped_players_1), f"opponent_{team2}"] = True

if f"opponent_{team1}" in opponent_cols:
    players_df.loc[players_df['player'].isin(mapped_players_2), f"opponent_{team1}"] = True
opponent_avg_dict = historical_data.groupby(['player','opponent'])['fantasy_points'].mean().to_dict()
player_avg_dict = (
    historical_data
    .groupby('player')['fantasy_points']
    .mean()
    .to_dict()
)
opponent_cols = [c for c in players_df.columns if c.startswith('opponent_')]
opponent_cols = opponent_cols[1:]
players_df['opponent_temp'] = (
    players_df[opponent_cols]
    .idxmax(axis=1)
    .str.replace('opponent_', '')
)
all_opponents = historical_data['opponent'].unique()
encoded_opponents = [c.replace('opponent_', '') for c in opponent_cols]

dropped_opponent = list(set(all_opponents) - set(encoded_opponents))[0]

mask = players_df[opponent_cols].sum(axis=1) == 0
players_df.loc[mask, 'opponent_temp'] = dropped_opponent
def get_opponent_avg(row):
    key = (row['player'], row['opponent_temp'])
    
    if key in opponent_avg_dict:
        return opponent_avg_dict[key]
    else:
        return player_avg_dict.get(row['player'], 0)
    
players_df = players_df.drop(columns = ['opponent_avg_points'])
players_df['opponent_avg_points'] = players_df.apply(get_opponent_avg, axis=1)
players_df = players_df.drop(columns = ['opponent_temp'])
pitch_info = historical_data[historical_data['venue'] == venue]['pitch_type'].mode()[0]
pitch_cols = [
    'pitch_type_batting_friendly',
    'pitch_type_pace_friendly',
    'pitch_type_spin_friendly'
]
players_df[pitch_cols] = False
if pitch_info == 'batting_friendly':
    players_df['pitch_type_batting_friendly'] = True
elif pitch_info == 'pace_friendly':
    players_df['pitch_type_pace_friendly'] = True
elif pitch_info == 'spin_friendly':
    players_df['pitch_type_spin_friendly'] = True
X_pred = players_df.drop(columns=['fantasy_points','player','venue','player_match_number','match_id','season','player_role_batsman','player_role_bowler'])
players_df['predicted_points'] = model.predict(X_pred)
players_df.sort_values(by= ['predicted_points'],ascending= False)
team_map = {}
for player in mapped_players_1:
    team_map[player] = team1
for player in mapped_players_2:
    team_map[player] = team2
players_df['team'] = players_df['player'].map(team_map)
player_stats = historical_data.groupby('player').agg({
    'batting_contribution_ratio': 'mean',
    'bowling_contribution_ratio': 'mean'
}).reset_index()
def assign_role(row):
    bat = row['batting_contribution_ratio']
    bowl = row['bowling_contribution_ratio']
    
    # All-rounder: contributes in BOTH
    if bat > 0.20 and bowl > 0.20:
        return 'AR'
    
    # Pure batsman
    elif bat >= bowl:
        return 'BAT'
    
    # Pure bowler
    else:
        return 'BOWL'
player_stats['role'] = player_stats.apply(assign_role, axis=1)
role_map = dict(zip(player_stats['player'], player_stats['role']))
players_df['role'] = players_df['player'].map(role_map).fillna('BAT')
players_df = players_df.sort_values(by='predicted_points', ascending=False).reset_index(drop=True)
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

    # ✅ Step 1: Fill minimum requirements
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

    # ✅ Step 2: Fill remaining slots (best players)
    for _, row in players_df.iterrows():
        if len(team) >= 11:
            break
            
        player = row['player']
        role = row['final_role']
        team_name = row['team']
        
        if player in team:
            continue
        
        # Team constraint
        if team_count.get(team_name, 0) >= 7:
            continue
        
        # Role max constraint
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