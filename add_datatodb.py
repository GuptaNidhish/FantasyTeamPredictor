import pandas as pd
from ingest_data import ingest_dataframe

df = pd.read_csv("/Users/nidhishgupta/Desktop/Dream11_Fantasy Team_Predictor/data/processed/database_csv.csv")

ingest_dataframe(df)