import pandas as pd 
import numpy as np 
data = pd.read_csv("/Users/nidhishgupta/Desktop/Dream11_Fantasy Team_Predictor/data/raw/IPL.csv")
batter_df = data.groupby(['match_id','batter']).agg(
    runs = ('runs_batter','sum'),
    balls = ('valid_ball','sum'),
    fours = ('runs_batter',lambda x:(x == 4).sum()),
    sixes = ('runs_batter',lambda x:(x == 6).sum())
).reset_index()
batter_df['strike_rate'] = (batter_df['runs'] / batter_df['balls']) * 100
bowler_df = data.groupby(['match_id', 'bowler']).agg(
    runs_conceded=('runs_bowler', 'sum'),
    balls_bowled=('valid_ball', 'sum'),
    wickets=('bowler_wicket', 'sum')
).reset_index()
bowler_df['overs'] = bowler_df['balls_bowled'] / 6
bowler_df['economy'] = bowler_df['runs_conceded'] / bowler_df['overs']
batter_df = batter_df.rename(columns={"batter": "player"})
bowler_df = bowler_df.rename(columns={"bowler": "player"})
player_match_df = batter_df.merge(
    bowler_df,
    on=["match_id", "player"],
    how="outer"
)
player_match_df = player_match_df.fillna(0)

