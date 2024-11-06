import pandas as pd
from sklearn.model_selection import train_test_split

df_movies = pd.read_csv("datasets/movie_metadata.csv")

print("Initial shape:", df_movies.shape)
print(df_movies.dtypes)

df_movies.drop(columns=['movie_imdb_link'], inplace=True)
df_movies.dropna(inplace=True)

print("Shape after cleaning:", df_movies.shape)

train_data, test_data = train_test_split(df_movies, test_size=0.3, random_state=42)

print("Train data shape:", train_data.shape)
print("Test data shape:", test_data.shape)
