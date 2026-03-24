import pandas as pd
from db.initialization import SessionLocal
from db.models import PlayerMatchStats, Match

def get_latest_players_df(player_list):
    """
    Fetch the last played match of each player in player_list from the DB.
    """
    db = SessionLocal()

    query = (
        db.query(
            PlayerMatchStats.player_name.label("player"),
            PlayerMatchStats.match_id,
            PlayerMatchStats.team,
            PlayerMatchStats.opponent,

            Match.venue,
            Match.pitch_type,

            PlayerMatchStats.runs,
            PlayerMatchStats.balls_played.label("balls"),
            PlayerMatchStats.fours,
            PlayerMatchStats.sixes,

            PlayerMatchStats.balls_bowled,
            PlayerMatchStats.runs_conceded,
            PlayerMatchStats.wickets,

            PlayerMatchStats.fielding_points,
            PlayerMatchStats.lbw_bonus,
            PlayerMatchStats.maiden_bonus,

            PlayerMatchStats.fantasy_points,
            PlayerMatchStats.player_match_number,
            PlayerMatchStats.batting_position.label("bat_pos"),
            PlayerMatchStats.is_wicketkeeper.label("player_role_wicketkeeper")
        )
        .join(Match, PlayerMatchStats.match_id == Match.match_id)
        .filter(PlayerMatchStats.player_name.in_(player_list))
    )

    df = pd.read_sql(query.statement, db.bind)
    db.close()

    # Pick last match per player
    df = (
        df.sort_values("player_match_number")
          .groupby("player")
          .tail(1)
          .reset_index(drop=True)
    )

    return df

def compute_features(players_df, db_session=None):
    """
    Placeholder function to compute derived features from DB.
    Later we will compute:
    - last3_avg_points
    - rolling_strike_rate
    - rolling_wickets
    - opponent_avg_points
    - venue_avg_points
    - contribution ratios, etc.
    """
    # For now, just return the base players_df
    return players_df
def compute_chunk1(players_df):
    """
    Compute first 3 derived features:
    - last3_avg_points (avg fantasy points of last 3 matches)
    - rolling_strike_rate (avg strike rate over last 5 matches)
    - rolling_wickets (avg wickets over last 5 matches)
    """
    db = SessionLocal()

    # Fetch historical data for all players in players_df
    player_names = players_df['player'].tolist()

    query = (
        db.query(
            PlayerMatchStats.player_name.label("player"),
            PlayerMatchStats.match_id,
            PlayerMatchStats.fantasy_points,
            PlayerMatchStats.runs,
            PlayerMatchStats.balls_played,
            PlayerMatchStats.wickets,
            PlayerMatchStats.player_match_number
        )
        .filter(PlayerMatchStats.player_name.in_(player_names))
    )

    hist_df = pd.read_sql(query.statement, db.bind)
    db.close()

    # Sort by match number
    hist_df = hist_df.sort_values(['player', 'player_match_number'])

    # --- last3_avg_points ---
    hist_df['last3_avg_points'] = (
        hist_df.groupby('player')['fantasy_points']
        .rolling(3, min_periods=1)
        .mean()
        .reset_index(level=0, drop=True)
    )

    # --- rolling_strike_rate over last 5 matches ---
    hist_df['strike_rate'] = hist_df['runs'] / hist_df['balls_played'] * 100
    hist_df['rolling_strike_rate'] = (
        hist_df.groupby('player')['strike_rate']
        .rolling(5, min_periods=1)
        .mean()
        .reset_index(level=0, drop=True)
    )

    # --- rolling_wickets over last 5 matches ---
    hist_df['rolling_wickets'] = (
        hist_df.groupby('player')['wickets']
        .rolling(5, min_periods=1)
        .mean()
        .reset_index(level=0, drop=True)
    )

    # Keep only the latest match row per player (to merge with base players_df)
    latest_features = hist_df.groupby('player').tail(1)

    # Merge with base players_df
    players_df = players_df.merge(
        latest_features[['player', 'last3_avg_points', 'rolling_strike_rate', 'rolling_wickets']],
        on='player',
        how='left'
    )

    return players_df
def compute_chunk2(players_df, team1, team2, venue):
    """
    Compute:
    - opponent_avg_points
    - venue_avg_points
    - venue_run_factor (match-level, correct definition)
    """
    db = SessionLocal()

    player_names = players_df['player'].tolist()

    # ==============================
    # 1. Fetch required historical data
    # ==============================
    query = (
        db.query(
            PlayerMatchStats.player_name.label("player"),
            PlayerMatchStats.match_id,   # ✅ REQUIRED FIX
            PlayerMatchStats.opponent,
            PlayerMatchStats.runs,
            PlayerMatchStats.fantasy_points,
            Match.venue
        )
        .join(Match, PlayerMatchStats.match_id == Match.match_id)
        .filter(PlayerMatchStats.player_name.in_(player_names))
    )

    hist_df = pd.read_sql(query.statement, db.bind)
    db.close()

    # ==============================
    # 2. Determine upcoming opponent
    # ==============================
    def get_opponent(row):
        return team2 if row['team'] == team1 else team1

    players_df['upcoming_opponent'] = players_df.apply(get_opponent, axis=1)

    # ==============================
    # 3. opponent_avg_points
    # ==============================
    opponent_avg = (
        hist_df
        .groupby(['player', 'opponent'])['fantasy_points']
        .mean()
        .to_dict()
    )

    player_avg = (
        hist_df
        .groupby('player')['fantasy_points']
        .mean()
        .to_dict()
    )

    def compute_opponent_avg(row):
        key = (row['player'], row['upcoming_opponent'])
        if key in opponent_avg:
            return opponent_avg[key]
        else:
            return player_avg.get(row['player'], 0)

    players_df['opponent_avg_points'] = players_df.apply(compute_opponent_avg, axis=1)

    # ==============================
    # 4. venue_avg_points
    # ==============================
    venue_avg = (
        hist_df
        .groupby(['player', 'venue'])['fantasy_points']
        .mean()
        .to_dict()
    )

    venue_global_avg = (
        hist_df
        .groupby('venue')['fantasy_points']
        .mean()
    )

    def compute_venue_avg(row):
        key = (row['player'], venue)
        if key in venue_avg:
            return venue_avg[key]
        else:
            return venue_global_avg.get(venue, venue_global_avg.mean())

    players_df['venue_avg_points'] = players_df.apply(compute_venue_avg, axis=1)

    # ==============================
    # 5. venue_run_factor (CORRECT)
    # ==============================

    # Step 1: total runs per match
    match_runs = (
        hist_df
        .groupby(['match_id', 'venue'])['runs']
        .sum()
        .reset_index()
    )

    # Step 2: avg runs per venue
    venue_runs_avg = match_runs.groupby('venue')['runs'].mean()

    # Step 3: overall avg runs
    overall_runs_avg = match_runs['runs'].mean()

    # Step 4: compute factor
    if venue in venue_runs_avg:
        players_df['venue_run_factor'] = venue_runs_avg[venue] / overall_runs_avg
    else:
        players_df['venue_run_factor'] = 1.0  # neutral fallback

    # ==============================
    # Cleanup
    # ==============================
    players_df = players_df.drop(columns=['upcoming_opponent'])

    return players_df
def compute_chunk3(players_df):
    """
    Compute:
    - batting_contribution_ratio (last 3 avg)
    - bowling_contribution_ratio (last 3 avg)
    - boundary_percentage (last 3 avg)
    """

    db = SessionLocal()
    player_names = players_df['player'].tolist()

    # ==============================
    # 1. Fetch raw historical data
    # ==============================
    query = (
        db.query(
            PlayerMatchStats.player_name.label("player"),
            PlayerMatchStats.runs,
            PlayerMatchStats.balls_played.label("balls"),
            PlayerMatchStats.fours,
            PlayerMatchStats.sixes,

            PlayerMatchStats.wickets,
            PlayerMatchStats.runs_conceded,
            PlayerMatchStats.balls_bowled,

            PlayerMatchStats.lbw_bonus.label("lbw_bowled_bonus"),
            PlayerMatchStats.maiden_bonus.label("maiden_points"),

            PlayerMatchStats.fielding_points,
            PlayerMatchStats.fantasy_points,
            PlayerMatchStats.player_match_number
        )
        .filter(PlayerMatchStats.player_name.in_(player_names))
    )

    hist_df = pd.read_sql(query.statement, db.bind)
    db.close()

    hist_df = hist_df.sort_values(['player', 'player_match_number'])

    # ==============================
    # 2. Batting Points
    # ==============================
    hist_df['batting_points'] = (
        hist_df['runs']
        + hist_df['fours']
        + hist_df['sixes'] * 2
    )

    duck_mask = (hist_df['runs'] == 0) & (hist_df['balls'] > 0)
    hist_df.loc[duck_mask, 'batting_points'] -= 2

    # ==============================
    # 3. Batting Bonus
    # ==============================
    hist_df['batting_bonus'] = 0
    hist_df.loc[hist_df['runs'] >= 30, 'batting_bonus'] = 4
    hist_df.loc[hist_df['runs'] >= 50, 'batting_bonus'] = 8
    hist_df.loc[hist_df['runs'] >= 100, 'batting_bonus'] = 16

    # ==============================
    # 4. Strike Rate Bonus
    # ==============================
    hist_df['strike_rate'] = hist_df['runs'] / hist_df['balls'] * 100
    hist_df['sr_bonus'] = 0

    mask = hist_df['balls'] >= 10

    hist_df.loc[mask & (hist_df['strike_rate'] < 50), 'sr_bonus'] = -6
    hist_df.loc[mask & (hist_df['strike_rate'].between(50, 59.99)), 'sr_bonus'] = -4
    hist_df.loc[mask & (hist_df['strike_rate'].between(60, 69.99)), 'sr_bonus'] = -2
    hist_df.loc[mask & (hist_df['strike_rate'].between(130, 149.99)), 'sr_bonus'] = 2
    hist_df.loc[mask & (hist_df['strike_rate'].between(150, 169.99)), 'sr_bonus'] = 4
    hist_df.loc[mask & (hist_df['strike_rate'] >= 170), 'sr_bonus'] = 6

    # ==============================
    # 5. Bowling Points
    # ==============================
    hist_df['bowling_points'] = hist_df['wickets'] * 25

    # ==============================
    # 6. Wicket Bonus
    # ==============================
    hist_df['wicket_bonus'] = 0
    hist_df.loc[hist_df['wickets'] >= 3, 'wicket_bonus'] = 4
    hist_df.loc[hist_df['wickets'] >= 4, 'wicket_bonus'] = 8
    hist_df.loc[hist_df['wickets'] >= 5, 'wicket_bonus'] = 16

    # ==============================
    # 7. Economy Bonus
    # ==============================
    hist_df['economy'] = (
        hist_df['runs_conceded'] / hist_df['balls_bowled'].replace(0, 1) * 6
    )

    hist_df['eco_bonus'] = 0
    mask = hist_df['balls_bowled'] >= 12

    hist_df.loc[mask & (hist_df['economy'] < 5), 'eco_bonus'] = 6
    hist_df.loc[mask & (hist_df['economy'].between(5, 5.99)), 'eco_bonus'] = 4
    hist_df.loc[mask & (hist_df['economy'].between(6, 6.99)), 'eco_bonus'] = 2
    hist_df.loc[mask & (hist_df['economy'].between(10, 11)), 'eco_bonus'] = -2
    hist_df.loc[mask & (hist_df['economy'].between(11.01, 12)), 'eco_bonus'] = -4
    hist_df.loc[mask & (hist_df['economy'] > 12), 'eco_bonus'] = -6

    # ==============================
    # 8. Contribution Ratios
    # ==============================
    denominator = hist_df['fantasy_points'] - hist_df['fielding_points']
    denominator = denominator.replace(0, 1)

    hist_df['batting_contribution_ratio'] = (
        hist_df['batting_points']
        + hist_df['batting_bonus']
        + hist_df['sr_bonus']
    ) / denominator

    hist_df['bowling_contribution_ratio'] = (
        hist_df['bowling_points']
        + hist_df['eco_bonus']
        + hist_df['wicket_bonus']
        + hist_df['lbw_bowled_bonus']
        + hist_df['maiden_points']
    ) / denominator

    # ==============================
    # 9. Boundary Percentage (CORRECTED)
    # ==============================
    hist_df['boundary_percentage'] = (
        (hist_df['fours'] * 4 + hist_df['sixes'] * 6) / hist_df['runs']
    )

    hist_df['boundary_percentage'] = hist_df['boundary_percentage'].fillna(0)

    # ==============================
    # 10. Rolling (last 3 matches)
    # ==============================
    for col in [
        'batting_contribution_ratio',
        'bowling_contribution_ratio',
        'boundary_percentage'
    ]:
        hist_df[col] = (
            hist_df.groupby('player')[col]
            .rolling(3, min_periods=1)
            .mean()
            .reset_index(level=0, drop=True)
        )

    # ==============================
    # 11. Take latest match values
    # ==============================
    latest = hist_df.groupby('player').tail(1)

    # ==============================
    # 12. Merge with players_df
    # ==============================
    players_df = players_df.merge(
        latest[['player',
                'batting_contribution_ratio',
                'bowling_contribution_ratio',
                'boundary_percentage']],
        on='player',
        how='left'
    )

    return players_df
def compute_chunk4(players_df):
    """
    Compute:
    - last10_std_points
    - player_consistency_index
    - form_momentum
    - recent_form
    - venue_form
    """

    db = SessionLocal()
    player_names = players_df['player'].tolist()

    # ==============================
    # 1. Fetch fantasy points history
    # ==============================
    query = (
        db.query(
            PlayerMatchStats.player_name.label("player"),
            PlayerMatchStats.fantasy_points,
            PlayerMatchStats.player_match_number
        )
        .filter(PlayerMatchStats.player_name.in_(player_names))
    )

    hist_df = pd.read_sql(query.statement, db.bind)
    db.close()

    hist_df = hist_df.sort_values(['player', 'player_match_number'])

    # ==============================
    # 2. Rolling stats
    # ==============================

    # last3 avg
    hist_df['last3_avg'] = (
        hist_df.groupby('player')['fantasy_points']
        .rolling(3, min_periods=1)
        .mean()
        .reset_index(level=0, drop=True)
    )

    # last5 avg
    hist_df['last5_avg'] = (
        hist_df.groupby('player')['fantasy_points']
        .rolling(5, min_periods=1)
        .mean()
        .reset_index(level=0, drop=True)
    )

    # last10 avg
    hist_df['last10_avg'] = (
        hist_df.groupby('player')['fantasy_points']
        .rolling(10, min_periods=1)
        .mean()
        .reset_index(level=0, drop=True)
    )

    # last10 std
    hist_df['last10_std_points'] = (
        hist_df.groupby('player')['fantasy_points']
        .rolling(10, min_periods=1)
        .std()
        .reset_index(level=0, drop=True)
        .fillna(0)
    )

    # ==============================
    # 3. Consistency Index
    # ==============================
    hist_df['player_consistency_index'] = (
        hist_df['last10_avg'] / (hist_df['last10_std_points'])
    )

    # ==============================
    # 4. Form Momentum
    # ==============================
    hist_df['form_momentum'] = (
        hist_df['last3_avg'] - hist_df['last10_avg']
    )

    # ==============================
    # 5. Recent Form
    # ==============================
    hist_df['recent_form'] = (
        0.6 * hist_df['last3_avg']
        + 0.3 * hist_df['last5_avg']
        + 0.1 * hist_df['last10_avg']
    )

    # ==============================
    # 6. Take latest values
    # ==============================
    latest = hist_df.groupby('player').tail(1)

    players_df = players_df.merge(
        latest[['player',
                'last10_std_points',
                'player_consistency_index',
                'form_momentum',
                'recent_form']],
        on='player',
        how='left'
    )

    # ==============================
    # 7. Venue Form
    # ==============================
    players_df['venue_form'] = (
        players_df['venue_avg_points'] * players_df['recent_form']
    )

    return players_df
if __name__ == "__main__":
    # Example usage:
    # List of players in upcoming match
    squad_players = ["Virat Kohli", "Rashid Khan", "MS Dhoni"]
    team1 = "Royal Challengers Bengaluru"
    team2 = "Sunrisers Hyderabad"
    mapped_venue = "M Chinnaswamy Stadium"
    # Step 1: get last played match for each player
    players_df = get_latest_players_df(squad_players)
    print("Base players_df:")
    print(players_df.head())

    # Step 2: compute derived features(chunk1)
    players_df = compute_chunk1(players_df)
    print("players_df with features:")
    print(players_df.head())
    # compute chunk 2
    players_df = compute_chunk2(players_df, team1, team2, mapped_venue)
    print(players_df[['player', 'opponent_avg_points', 'venue_avg_points', 'venue_run_factor']])
    #Compute chunk 3
    players_df = compute_chunk3(players_df)
    print(players_df[['player', 'batting_contribution_ratio','bowling_contribution_ratio','boundary_percentage']])
    #compute chunk 4
    players_df = compute_chunk4(players_df)
    print(players_df[['player','last10_std_points','player_consistency_index','form_momentum','recent_form','venue_form']])
    print(players_df.isnull().sum())
