import numpy as np 
import pandas as pd 
data = pd.read_csv('/Users/nidhishgupta/Desktop/Dream11_Fantasy Team_Predictor/data/half_processed/half_prepared_data.csv')
print(data.isnull().sum())
