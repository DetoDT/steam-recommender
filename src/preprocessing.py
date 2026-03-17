import pandas as pd
import numpy as np
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DATA_FOLDER = PROJECT_ROOT / "data" / "raw" 
OUTPUT_FOLDER = PROJECT_ROOT / "data" / "processed"

users_df = pd.read_csv(RAW_DATA_FOLDER / "users_dummy.csv")
games_df = pd.read_csv(RAW_DATA_FOLDER / "games_dummy.csv")
recommendations_df = pd.read_csv(RAW_DATA_FOLDER / "recommendations_dummy.csv")

output_path = '../data/processed/' 

###
# DATA CLEANING
###

## dropping duplicates
games_df = games_df.drop_duplicates()
users_df = users_df.drop_duplicates()
recommendations_df = recommendations_df.drop_duplicates()

## Replace missing/wrong date with a sentinel value
games_df['date_release'] = pd.to_datetime(games_df['date_release'], format='%Y-%m-%d', errors='coerce')
games_df["date_release"] = games_df["date_release"].fillna(pd.Timestamp("1900-01-01"))


## win, mac, linux, steam_deck missing or not boolean
cols = ["win", "mac", "linux", "steam_deck"]
games_df[cols] = games_df[cols].fillna(False)
games_df[cols] = games_df[cols].where(games_df[cols].isin([True, False]), False)

## Notify if popular game contiains missing fields

mask = (games_df['user_reviews'] >= 10000) & (games_df.isna().any(axis=1))
problem_rows = games_df.loc[mask]
problem_rows.to_csv(OUTPUT_FOLDER / "missing_high_review_entries.log", index=False)

## Deleting entries with missing fields

print(games_df.isna().sum())
print()
if input("Drop values? [y/N]") == 'y':
    games_df = games_df.dropna()

## Transform Steam rating into a number

rating_map = {
    "Very Negative": 0,
    "Negative": 1,
    "Mostly Negative": 2,
    "Mixed": 3,
    "Mostly Positive": 4,
    "Positive": 5,
    "Very Positive": 6,
    "Overwhelmingly Positive": 7
}

games_df["rating_score"] = games_df["rating"].map(rating_map)

## Deleting entries containing missing values for the other files

print(users_df.isna().sum())
print()
if input("Drop values? [y/N]") == 'y':
    users_df = users_df.dropna()
print(recommendations_df.isna().sum())
print()
if (input("Drop values? [y/N]") == 'y'):
    recommendations_df = recommendations_df.dropna()

## remove games with less than 100 reviews
games_df= games_df[games_df['user_reviews'] >= 100]

## remove users with less than 5 products
users_df = users_df[users_df['products'] >= 5]

## remove from recommendations all entries that contain a gameid / userid not present in the other files
recommendations_df = recommendations_df.merge(users_df[['user_id']], on='user_id')
recommendations_df = recommendations_df.merge(games_df[['app_id']], on='app_id')

## export
users_df.to_csv(OUTPUT_FOLDER / 'users_dummy.csv')
games_df.to_csv(OUTPUT_FOLDER / 'games_dummy.csv')
recommendations_df.to_csv(OUTPUT_FOLDER / 'recommendations_dummy.csv')