import numpy as np 
import pandas as pd 
from lightgbm import LGBMRegressor
import joblib
data = pd.read_csv("/Users/nidhishgupta/Desktop/Dream11_Fantasy Team_Predictor/data/processed/ready_for_training.csv")
X_train = data.drop(columns=['fantasy_points','player','venue','season','player_role_batsman','player_role_bowler','match_id'])
y_train = data['fantasy_points']
best_params = {'subsample': 0.9, 'num_leaves': 127, 'min_child_samples': 5, 'max_depth': 10, 'learning_rate': 0.02, 'colsample_bytree': 0.6}
final_model = LGBMRegressor(
    n_estimators=600,
    random_state=42,
    reg_alpha = 0.1,
    reg_lambda = 1,
    **best_params
)

final_model.fit(
    X_train,
    y_train,
)
joblib.dump(final_model,"/Users/nidhishgupta/Desktop/Dream11_Fantasy Team_Predictor/models/point_predicter.pkl")
