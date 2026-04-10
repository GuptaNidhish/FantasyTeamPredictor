def retrain_model():
    print("🚀 Starting model retraining...")

    import pandas as pd
    import numpy as np
    from sqlalchemy.orm import Session
    from db.initialization import SessionLocal
    from db.models import PlayerMatchStats, Match
    from db.computefeatures import featureFor_retraining
    from lightgbm import LGBMRegressor
    import io
    import joblib
    from supabase import create_client
    import os
    import requests
    import pickle

    session: Session = SessionLocal()

    MODEL_COLS_URL = "https://mpyitncpkyunkqccefit.supabase.co/storage/v1/object/public/Models/model_cols.pkl"
    MODEL_COLS_PATH = "modelcols.pkl"

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
                "team": r.team,
                "opponent": r.opponent,
                "player_role_wicketkeeper": r.is_wicketkeeper,
                "runs": r.runs,
                "balls_played": r.balls_played,
                "fours": r.fours,
                "sixes": r.sixes,
                "balls_bowled": r.balls_bowled,
                "runs_conceded": r.runs_conceded,
                "wickets": r.wickets,
                "maidens": r.maiden_bonus,
                "fielding_points": r.fielding_points,
                "batting_position": r.batting_position,
                "player_match_number": r.player_match_number,
                "fantasy_points": r.fantasy_points
            })

        df = pd.DataFrame(data)
        print(f"📊 Total rows fetched: {len(df)}")

        # ================= ADD MATCH CONTEXT =================
        matches = session.query(Match).all()

        match_map = {
            m.match_id: {
                "venue": m.venue,
                "pitch_type": m.pitch_type
            }
            for m in matches
        }

        df["venue"] = df["match_id"].map(lambda x: match_map.get(x, {}).get("venue"))
        df["pitch_type"] = df["match_id"].map(lambda x: match_map.get(x, {}).get("pitch_type"))

        # ✅ Handle missing values
        df["venue"] = df["venue"].fillna("Unknown")
        df["pitch_type"] = df["pitch_type"].fillna("Unknown")

        # ================= FEATURE ENGINEERING =================
        df = featureFor_retraining(df)

        # ================= ENCODING =================
        df_encoded = pd.get_dummies(
            df,
            columns=['team', 'opponent', 'pitch_type']
        )

        # ================= MODEL COLS =================
        def download_model_cols():
            print("Downloading model cols from Supabase...")
            response = requests.get(MODEL_COLS_URL)
            if response.status_code == 200:
                with open(MODEL_COLS_PATH, "wb") as f:
                    f.write(response.content)
                print("Model cols downloaded successfully!")
            else:
                raise Exception("Failed to download model cols")

        if not os.path.exists(MODEL_COLS_PATH):
            download_model_cols()

        with open(MODEL_COLS_PATH, "rb") as f:
            model_columns = pickle.load(f)

        for col in model_columns:
            if col not in df_encoded.columns:
                df_encoded[col] = 0

        df_encoded = df_encoded[model_columns]

        # ================= CLEANING =================
        df_encoded = df_encoded.apply(pd.to_numeric, errors='coerce').fillna(0)

        # ================= TARGET =================
        X = df_encoded
        y = df["fantasy_points"]

        # ================= MODEL =================
        best_params = {
            'subsample': 0.9,
            'num_leaves': 135,
            'min_child_samples': 5,
            'max_depth': 10,
            'learning_rate': 0.02,
            'colsample_bytree': 0.6
        }

        final_model = LGBMRegressor(
            n_estimators=600,
            random_state=42,
            reg_alpha=0.1,
            reg_lambda=1,
            **best_params
        )

        final_model.fit(X, y)
        print("Model trained")

        # ================= SAVE MODEL TO SUPABASE =================
        url = "https://mpyitncpkyunkqccefit.supabase.co"
        key = os.getenv("SUPABASE_KEY")

        supabase = create_client(url, key)

        # ✅ Use BytesIO correctly
        buffer = io.BytesIO()
        joblib.dump(final_model, buffer)

        supabase.storage.from_("Models").upload(
            "point_predicter_final.pkl",
            buffer.getvalue(),   # ✅ DEPLOYMENT SAFE
            {"upsert": "true"}
        )

        print("💾 Model saved successfully")

    except Exception as e:
        print(f"❌ Error during retraining: {e}")

    finally:
        session.close()