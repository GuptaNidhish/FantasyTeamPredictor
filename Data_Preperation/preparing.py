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
catches = (
    data[data["wicket_kind"] == "caught"]
    .groupby(["match_id", "fielders"])
    .size()
    .reset_index(name="catches")
)
stumpings = (
    data[data["wicket_kind"] == "stumped"]
    .groupby(["match_id", "fielders"])
    .size()
    .reset_index(name="stumpings")
)
runout_raw = data[data["wicket_kind"] == "run out"][["match_id", "fielders"]].copy()
runout_raw = runout_raw.dropna(subset=["fielders"])
runout_raw["num_fielders"] = runout_raw["fielders"].str.count(",") + 1
runout_raw["fielders"] = runout_raw["fielders"].str.split(",")
runout_raw = runout_raw.explode("fielders")
runout_raw["fielders"] = runout_raw["fielders"].str.strip()
runout_raw["runout_points"] = 0
runout_raw.loc[runout_raw["num_fielders"] == 1, "runout_points"] = 12
runout_raw.loc[runout_raw["num_fielders"] > 1, "runout_points"] = 6
runout_points = (
    runout_raw.groupby(["match_id", "fielders"])["runout_points"]
    .sum()
    .reset_index()
)
runout_points = runout_points.rename(columns={"fielders": "player"})
catches = catches.rename(columns={"fielders": "player"})
stumpings = stumpings.rename(columns={"fielders": "player"})
fielding_df = catches.merge(stumpings,on = ['match_id','player'],how = 'outer')
fielding_df = fielding_df.merge(runout_points,on=["match_id", "player"],how="outer")
fielding_df = fielding_df.fillna(0)
fielding_df['fielding_points'] = fielding_df['catches']*8 + fielding_df['stumpings']*12 + fielding_df["runout_points"]
fielding_df.loc[fielding_df['catches']>=3,'fielding_points'] += 4
player_match_df = player_match_df.merge(
    fielding_df[["match_id", "player", "fielding_points"]],
    on=["match_id", "player"],
    how="left"
)
player_match_df['fielding_points'] = player_match_df['fielding_points'].fillna(0)
player_match_df["batting_points"] = (
    player_match_df["runs"]
    + player_match_df["fours"] * 1
    + player_match_df["sixes"] * 2
)
duck_mask = (player_match_df["runs"] == 0) & (player_match_df["balls"] > 0)
player_match_df.loc[duck_mask, "batting_points"] -= 2
player_match_df["batting_bonus"] = 0
player_match_df.loc[player_match_df["runs"] >= 30, "batting_bonus"] = 4
player_match_df.loc[player_match_df["runs"] >= 50, "batting_bonus"] = 8
player_match_df.loc[player_match_df["runs"] >= 100, "batting_bonus"] = 16
player_match_df["sr_bonus"] = 0
mask = player_match_df["balls"] >= 10
player_match_df.loc[mask & (player_match_df["strike_rate"] < 50), "sr_bonus"] = -6
player_match_df.loc[mask & (player_match_df["strike_rate"].between(50,59.99)), "sr_bonus"] = -4
player_match_df.loc[mask & (player_match_df["strike_rate"].between(60,69.99)), "sr_bonus"] = -2
player_match_df.loc[mask & (player_match_df["strike_rate"].between(130,149.99)), "sr_bonus"] = 2
player_match_df.loc[mask & (player_match_df["strike_rate"].between(150,169.99)), "sr_bonus"] = 4
player_match_df.loc[mask & (player_match_df["strike_rate"] >= 170), "sr_bonus"] = 6
player_match_df["wicket_bonus"] = 0
player_match_df.loc[player_match_df["wickets"] >= 3, "wicket_bonus"] = 4
player_match_df.loc[player_match_df["wickets"] >= 4, "wicket_bonus"] = 8
player_match_df.loc[player_match_df["wickets"] >= 5, "wicket_bonus"] = 16
player_match_df["eco_bonus"] = 0
mask = player_match_df["balls_bowled"] >= 12
player_match_df.loc[mask & (player_match_df["economy"] < 5), "eco_bonus"] = 6
player_match_df.loc[mask & (player_match_df["economy"].between(5,5.99)), "eco_bonus"] = 4
player_match_df.loc[mask & (player_match_df["economy"].between(6,6.99)), "eco_bonus"] = 2
player_match_df.loc[mask & (player_match_df["economy"].between(10,11)), "eco_bonus"] = -2
player_match_df.loc[mask & (player_match_df["economy"].between(11.01,12)), "eco_bonus"] = -4
player_match_df.loc[mask & (player_match_df["economy"] > 12), "eco_bonus"] = -6
lbw_bowled = data[
    data["wicket_kind"].isin(["bowled", "lbw"])
]
lbw_bowled = (
    lbw_bowled
    .groupby(["match_id", "bowler"])
    .size()
    .reset_index(name="lbw_bowled_wickets")
)
lbw_bowled["lbw_bowled_bonus"] = lbw_bowled["lbw_bowled_wickets"] * 8
lbw_bowled = lbw_bowled.rename(columns={"bowler": "player"})
player_match_df = player_match_df.merge(
    lbw_bowled[["match_id", "player", "lbw_bowled_bonus"]],
    on=["match_id", "player"],
    how="left"
)
player_match_df["lbw_bowled_bonus"] = player_match_df["lbw_bowled_bonus"].fillna(0)
player_match_df["bowling_points"] = player_match_df["wickets"] * 25
valid_balls = data[data["valid_ball"] == 1]
over_runs = (
    valid_balls
    .groupby(["match_id", "bowler", "over"])
    .agg(
        balls=("valid_ball", "count"),
        runs=("runs_bowler", "sum")
    )
    .reset_index()
)
maiden_overs = over_runs[
    (over_runs["balls"] == 6) &
    (over_runs["runs"] == 0)
]
maiden_overs = (
    maiden_overs
    .groupby(["match_id", "bowler"])
    .size()
    .reset_index(name="maidens")
)
maiden_overs["maiden_points"] = maiden_overs["maidens"] * 12
maiden_overs = maiden_overs.rename(columns={"bowler": "player"})
player_match_df = player_match_df.merge(
    maiden_overs[["match_id", "player", "maiden_points"]],
    on=["match_id", "player"],
    how="left"
)
player_match_df["maiden_points"] = player_match_df["maiden_points"].fillna(0)
player_match_df["fantasy_points"] = (
    player_match_df["batting_points"]
    + player_match_df["batting_bonus"]
    + player_match_df["sr_bonus"]
    + player_match_df["bowling_points"]
    + player_match_df["wicket_bonus"]
    + player_match_df["eco_bonus"]
    + player_match_df["lbw_bowled_bonus"]
    + player_match_df["maiden_points"]
    + player_match_df["fielding_points"]
)
match_meta = data[[
    "match_id",
    "venue",
    "city",
    "toss_winner",
    "toss_decision",
    "stage",
    "season"
]].drop_duplicates()
player_team = data[[
    "match_id",
    "batter",
    "bowler",
    "batting_team",
    "bowling_team"
]]
batter_team = player_team[["match_id","batter","batting_team","bowling_team"]]
batter_team = batter_team.rename(columns={
    "batter":"player",
    "batting_team":"team",
    "bowling_team":"opponent"
})
bowler_team = player_team[["match_id","bowler","batting_team","bowling_team"]]
bowler_team = bowler_team.rename(columns={
    "bowler":"player",
    "bowling_team":"team",
    "batting_team":"opponent"
})
player_teams = pd.concat([batter_team, bowler_team])
player_teams = player_teams.drop_duplicates()
player_match_df = player_match_df.merge(
    player_teams,
    on=["match_id","player"],
    how="left"
)
player_match_df = player_match_df.merge(
    match_meta,
    on="match_id",
    how="left"
)
match_date = data[["match_id", "date"]].drop_duplicates()
player_match_df = player_match_df.merge(match_date, on="match_id", how="left")
player_match_df = player_match_df.sort_values(["player", "date"])
player_match_df["player_match_number"] = (
    player_match_df.groupby("player").cumcount() + 1
)
player_match_df["last3_avg_points"] = (
    player_match_df
    .groupby("player")["fantasy_points"]
    .rolling(3)
    .mean()
    .shift(1)
    .reset_index(level=0, drop=True)
)
player_match_df["last5_avg_points"] = (
    player_match_df
    .groupby("player")["fantasy_points"]
    .rolling(5)
    .mean()
    .shift(1)
    .reset_index(level=0, drop=True)
)
player_match_df["last10_avg_points"] = (
    player_match_df
    .groupby("player")["fantasy_points"]
    .rolling(10)
    .mean()
    .shift(1)
    .reset_index(level=0, drop=True)
)
player_match_df["rolling_strike_rate"] = (
    player_match_df
    .groupby("player")["strike_rate"]
    .rolling(5)
    .mean()
    .shift(1)
    .reset_index(level=0, drop=True)
)
player_match_df["rolling_wickets"] = (
    player_match_df
    .groupby("player")["wickets"]
    .rolling(5)
    .mean()
    .shift(1)
    .reset_index(level=0, drop=True)
)
venue_avg = (
    player_match_df
    .groupby(["player", "venue"])["fantasy_points"]
    .mean()
    .reset_index(name="venue_avg_points")
)
player_match_df = player_match_df.merge(
    venue_avg,
    on=["player", "venue"],
    how="left"
)
opp_avg = (
    player_match_df
    .groupby(["player", "opponent"])["fantasy_points"]
    .mean()
    .reset_index(name="opponent_avg_points")
)
player_match_df = player_match_df.merge(
    opp_avg,
    on=["player", "opponent"],
    how="left"
)
