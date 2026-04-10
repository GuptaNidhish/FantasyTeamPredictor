def retrain_model():
    print("🚀 Starting model retraining...")

    import pandas as pd
    import numpy as np
    from sqlalchemy.orm import Session
    from db.initialization import SessionLocal
    from db.models import PlayerMatchStats, Match
    from db.computefeatures import featureFor_retraining  # ✅ SAME as inference
    from lightgbm import LGBMRegressor
    import io
    import joblib
    from supabase import create_client
    import os
    session: Session = SessionLocal()

    try:
        # ================= FETCH DATA =================
        records = session.query(PlayerMatchStats).all()

        if not records:
            print("❌ No data found in DB")
            return

        # ================= BUILD DATAFRAME =================
        data = []
        for r in records:
            data.append({
                "player": r.player_name,
                "match_id": r.match_id,

                # Context (✅ already available in your schema)
                "team": r.team,
                "opponent": r.opponent,
                "player_role_wicketkeeper": r.is_wicketkeeper,

                # Batting
                "runs": r.runs,
                "balls_played": r.balls_played,
                "fours": r.fours,
                "sixes": r.sixes,

                # Bowling
                "balls_bowled": r.balls_bowled,
                "runs_conceded": r.runs_conceded,
                "wickets": r.wickets,
                "maidens": r.maiden_bonus,  # mapped

                # Fielding
                "fielding_points": r.fielding_points,

                # Context features
                "batting_position": r.batting_position,
                "player_match_number": r.player_match_number,

                # TARGET
                "fantasy_points": r.fantasy_points
            })

        df = pd.DataFrame(data)

        print(f"📊 Total rows fetched: {len(df)}")

        # ================= ADD MATCH CONTEXT =================
        matches = session.query(Match).all()

        match_map = {
            m.id: {
                "venue": m.venue,
                "pitch_type": m.pitch_type
            }
            for m in matches
        }

        df["venue"] = df["match_id"].map(lambda x: match_map.get(x, {}).get("venue"))
        df["pitch_type"] = df["match_id"].map(lambda x: match_map.get(x, {}).get("pitch_type"))
        # ================= FEATURE ENGINEERING =================
        df = featureFor_retraining(df)
        # ================= ENCODING =================
        df_encoded = pd.get_dummies(
            df,
            columns=['team', 'opponent', 'pitch_type']
        )

        # ================= ALIGN WITH MODEL COLS =================
        model_cols_path = "https://mpyitncpkyunkqccefit.supabase.co/storage/v1/object/public/Models/model_cols.pkl"
        model_columns = joblib.load(model_cols_path)

        for col in model_columns:
            if col not in df_encoded.columns:
                df_encoded[col] = 0

        df_encoded = df_encoded[model_columns]

        # ================= CLEANING =================
        df_encoded = df_encoded.apply(pd.to_numeric, errors='coerce').fillna(0)

        # ================= TARGET =================
        TARGET = "fantasy_points"

        X = df_encoded
        y = df[TARGET]
        # ================= MODEL =================
        best_params = {'subsample': 0.9, 'num_leaves': 135, 'min_child_samples': 5, 'max_depth': 10, 'learning_rate': 0.02, 'colsample_bytree': 0.6}
        final_model = LGBMRegressor(
            n_estimators=600,
            random_state=42,
            reg_alpha = 0.1,
            reg_lambda = 1,
            **best_params
        )   

        final_model.fit(X, y)
        print("Model trained")

        # ================= SAVE MODEL =================
        url = "https://mpyitncpkyunkqccefit.supabase.co"
        key = os.getenv("SUPABASE_KEY")

        supabase = create_client(url, key)

        buffer = io.BytesIO()
        joblib.dump(final_model, buffer)
        buffer.seek(0)

        supabase.storage.from_("Models").upload(
            "point_predicter_final.pkl",
            buffer,
            {"upsert": True}   # 🔥 this overwrites existing file
        )

        print("💾 Model saved successfully")

    except Exception as e:
        print(f"❌ Error during retraining: {e}")

    finally:
        session.close()