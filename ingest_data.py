import pandas as pd
from db.initialization import SessionLocal
from db.models import Match, PlayerMatchStats


# ✅ Convert numpy → python native
def to_python_type(value):
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


def ingest_dataframe(df: pd.DataFrame):
    db = SessionLocal()

    try:
        # ==============================
        # ✅ 0. Convert DataFrame types
        # ==============================
        df = df.applymap(lambda x: x.item() if hasattr(x, "item") else x)

        # ==============================
        # ✅ 1. INSERT MATCHES
        # ==============================
        match_ids = df['match_id'].unique()

        for match_id in match_ids:
            match_id = str(to_python_type(match_id))

            match_rows = df[df['match_id'] == int(match_id)]

            teams = match_rows['team'].unique()

            if len(teams) != 2:
                raise ValueError(f"Invalid teams for match_id {match_id}")

            team1, team2 = sorted(teams)

            venue = to_python_type(match_rows.iloc[0]['venue'])
            pitch_type = to_python_type(match_rows.iloc[0]['pitch_type'])

            match = Match(
                match_id=match_id,
                team1=team1,
                team2=team2,
                venue=venue,
                pitch_type=pitch_type
            )

            db.merge(match)

        # ==============================
        # ✅ 2. INSERT / UPDATE PLAYERS
        # ==============================
        for _, row in df.iterrows():

            match_id = str(to_python_type(row['match_id']))
            player_name = to_python_type(row['player'])

            # 🔍 Check if already exists
            existing = db.query(PlayerMatchStats).filter_by(
                match_id=match_id,
                player_name=player_name
            ).first()

            if existing:
                # ✅ UPDATE
                existing.team = to_python_type(row['team'])
                existing.opponent = to_python_type(row['opponent'])

                existing.runs = to_python_type(row['runs'])
                existing.balls_played = to_python_type(row['balls'])
                existing.fours = to_python_type(row['fours'])
                existing.sixes = to_python_type(row['sixes'])

                existing.balls_bowled = to_python_type(row['balls_bowled'])
                existing.runs_conceded = to_python_type(row['runs_conceded'])
                existing.wickets = to_python_type(row['wickets'])

                existing.fielding_points = to_python_type(row['fielding_points'])
                existing.lbw_bonus = to_python_type(row['lbw_bowled_bonus'])
                existing.maiden_bonus = to_python_type(row['maiden_points'])

                existing.fantasy_points = to_python_type(row['fantasy_points'])

                existing.player_match_number = to_python_type(row['player_match_number'])
                existing.batting_position = to_python_type(row['bat_pos'])
                existing.is_wicketkeeper = to_python_type(row['player_role_wicketkeeper'])

            else:
                # ✅ INSERT
                player = PlayerMatchStats(
                    match_id=match_id,
                    player_name=player_name,

                    team=to_python_type(row['team']),
                    opponent=to_python_type(row['opponent']),

                    runs=to_python_type(row['runs']),
                    balls_played=to_python_type(row['balls']),
                    fours=to_python_type(row['fours']),
                    sixes=to_python_type(row['sixes']),

                    balls_bowled=to_python_type(row['balls_bowled']),
                    runs_conceded=to_python_type(row['runs_conceded']),
                    wickets=to_python_type(row['wickets']),

                    fielding_points=to_python_type(row['fielding_points']),
                    lbw_bonus=to_python_type(row['lbw_bowled_bonus']),
                    maiden_bonus=to_python_type(row['maiden_points']),

                    fantasy_points=to_python_type(row['fantasy_points']),

                    player_match_number=to_python_type(row['player_match_number']),
                    batting_position=to_python_type(row['bat_pos']),
                    is_wicketkeeper=to_python_type(row['player_role_wicketkeeper'])
                )

                db.add(player)

        # ==============================
        # ✅ COMMIT
        # ==============================
        db.commit()
        print("✅ Data inserted successfully!")

    except Exception as e:
        db.rollback()
        print("❌ Error during ingestion:", e)

    finally:
        db.close()

