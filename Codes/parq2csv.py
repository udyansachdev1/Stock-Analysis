import pandas as pd

df = pd.read_parquet("./data/processed_20.parquet")
df.to_csv("./data/processed_20.csv", index=False)
