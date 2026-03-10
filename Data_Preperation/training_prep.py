import numpy as np 
import pandas as pd
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import OneHotEncoder
import joblib
data = pd.read_csv('/Users/nidhishgupta/Desktop/Dream11_Fantasy Team_Predictor/data/half_processed/half_prepared_data.csv')
team_map = {
    "Royal Challengers Bangalore": "Royal Challengers Bengaluru",
    "Kings XI Punjab": "Punjab Kings",
    "Delhi Daredevils": "Delhi Capitals",
    "Rising Pune Supergiant": "Rising Pune Supergiants"
}
data['team'] = data['team'].replace(team_map)
data['opponent'] = data['opponent'].replace(team_map)
data['toss_winner'] = data['toss_winner'].replace(team_map)
avg_fantasy_points=data['fantasy_points'].mean()
data[['last3_avg_points','last5_avg_points','last10_avg_points']] = data[['last3_avg_points','last5_avg_points','last10_avg_points']].fillna(avg_fantasy_points)
data['strike_rate'] = data['strike_rate'].replace([np.inf, -np.inf], 0)
avg_strike_rate = data['strike_rate'].mean()
data['rolling_strike_rate'] = data['rolling_strike_rate'].fillna(avg_strike_rate)
data['rolling_wickets'] = data['rolling_wickets'].fillna(0)
data['last10_std_points'] = data['last10_std_points'].fillna(0)
data['form_momentum'] = data['form_momentum'].fillna(0)
data['bat_pos'] = data['bat_pos'].fillna(11)
data['bat_pos_per_match'] = data['bat_pos_per_match'].fillna(11)
cols_to_drop = [
'runs','balls','fours','sixes','strike_rate',
'runs_conceded','balls_bowled','wickets','overs','economy',
'batting_points','bowling_points','fielding_points',
'batting_bonus','sr_bonus','wicket_bonus','eco_bonus',
'lbw_bowled_bonus','maiden_points'
]
data = data.drop(columns = cols_to_drop)
data['date'] = pd.to_datetime(data['date'])
data['match_month'] = data['date'].dt.month
data = data.drop(columns=['date','match_id'])
data.to_csv("/Users/nidhishgupta/Desktop/Dream11_Fantasy Team_Predictor/data/processed/data_processed.csv",index = False)
data['team_won_toss'] = (
    data['team'] == data['toss_winner']
).astype(int)
data = data.drop(columns=['toss_winner','city'])
one_hot_cols = [
    'team',
    'opponent',
    'toss_decision',
    'stage',
    'pitch_type',
    'player_role'
]
le_player = LabelEncoder()
data['player'] = le_player.fit_transform(data['player'])
le_venue = LabelEncoder()
data['venue'] = le_venue.fit_transform(data['venue'])
data = pd.get_dummies(
    data,
    columns=one_hot_cols,
    drop_first=True
)
data.to_csv("/Users/nidhishgupta/Desktop/Dream11_Fantasy Team_Predictor/data/processed/ready_for_training.csv",index = False)
joblib.dump(le_player,"/Users/nidhishgupta/Desktop/Dream11_Fantasy Team_Predictor/encoders/player_encoder.pkl")
joblib.dump(le_venue,"/Users/nidhishgupta/Desktop/Dream11_Fantasy Team_Predictor/encoders/venue_encoder.pkl")
