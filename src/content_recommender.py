import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_FOLDER = PROJECT_ROOT / "data" / "processed"

# users_df = pd.read_csv(DATA_FOLDER / "users.csv")
games_df = pd.read_csv(DATA_FOLDER / "games.csv")
recommendations_df = pd.read_csv(DATA_FOLDER / "recommendations.csv")

# Collaborative filtering

# score = 1 + (helpful/helpful+funny - funny/helpful+funny) || -1 - (helpful/helpful+funny - funny/helpful+funny)
recommendations_df['overall_score'] = (
    (2 * recommendations_df['is_recommended'] - 1)
    + (
        recommendations_df['helpful'] /
        (1 + recommendations_df['helpful'] + recommendations_df['funny'])
        -
        recommendations_df['funny'] /
        (1 + recommendations_df['helpful'] + recommendations_df['funny'])
    )
)
user_game = recommendations_df.pivot_table(index='user_id', columns='app_id', values='overall_score', fill_value=0)


# Computer how similar is each user to each other
user_similarity = cosine_similarity(user_game)
user_similarity_df = pd.DataFrame(user_similarity, index=user_game.index, columns=user_game.index)

# Given target_user, find 10 most similar users
target_user = 123

similar_users = (
    user_similarity_df[target_user]
    .sort_values(ascending=False)
    .iloc[1:11]
)

print(similar_users)
# To recommend games, find the games the most similar users like that target user hasnt played
