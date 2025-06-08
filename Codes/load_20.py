"""
To Load the past 20 years from the API and save it as a csv file
"""

import pandas as pd

# Load list of tickers
tickers = pd.read_csv("./Data/Tickers.csv")

counter = 1
for i in tickers["Ticker"]:
    # try and except to avoid wasting API calls
    try:
        query = (
            "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol="
            + i
            + "&outputsize=full&apikey=B434MFNHO276GM45&datatype=csv"
        )

        # Load the data from the API
        df = pd.read_csv(query)

        # Adding column for ticker
        df["Ticker"] = i

        # Save the data as a csv file
        file_path = "./Data/raw_20/" + str(counter).zfill(2) + "_" + i + ".csv"

        # Save the data as a csv file
        df.to_csv(file_path, index=False)
        print(f"Saved {i}")
        counter += 1
    except:
        print(f"Error occoured for {i}")
        break
print("Process Completed")

# ruff: noqa
