import numpy as np 
import pandas as pd 
data = pd.read_csv("/Users/nidhishgupta/Desktop/Dream11_Fantasy Team_Predictor/data/raw/IPL.csv")
print(data.head())
print(data.isnull().sum())