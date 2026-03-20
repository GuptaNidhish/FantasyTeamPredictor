import numpy as np 
import pandas as pd
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import OneHotEncoder
import joblib
data = pd.read_csv('/Users/nidhishgupta/Desktop/Dream11_Fantasy Team_Predictor/data/half_processed/half_prepared_data.csv')
raw_data = pd.read_csv("/Users/nidhishgupta/Desktop/Dream11_Fantasy Team_Predictor/data/raw/IPL.csv")
wk_list = raw_data.loc[raw_data['wicket_kind']=='stumped','fielders'].unique()
data["player_role_wicketkeeper"] = data["player"].isin(wk_list).astype(int)
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
data = data.drop(columns=['date'])
data.to_csv("/Users/nidhishgupta/Desktop/Dream11_Fantasy Team_Predictor/data/processed/data_processed.csv",index = False)
data = data.drop(columns=['toss_winner','city','toss_decision'])
data['recent_form'] = (data['last3_avg_points']*0.6 + data['last5_avg_points']*0.3 + data['last10_avg_points']*0.1)
data['venue_form'] = data['venue_avg_points'] * data['recent_form']
data.to_csv("/Users/nidhishgupta/Desktop/Dream11_Fantasy Team_Predictor/data/processed/historical_data.csv",index = False)
one_hot_cols = [
    'team',
    'opponent',
    'stage',
    'pitch_type',
    'player_role'
]
data = pd.get_dummies(
    data,
    columns=one_hot_cols,
    drop_first=True
)
colsToDrop =  ['last5_avg_points',
              'last10_avg_points',
              'bat_pos_per_match',
              'match_month',
              'stage_Elimination Final',
              'stage_Eliminator',
              'stage_Final',
              'stage_Qualifier 1',
              'stage_Qualifier 2',
              'stage_Semi Final',
              'stage_Unknown',
              ]
data = data.drop(columns = colsToDrop)
data.to_csv("/Users/nidhishgupta/Desktop/Dream11_Fantasy Team_Predictor/data/processed/full_req_data.csv",index = False)
