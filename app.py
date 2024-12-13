from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pickle
import numpy as np
import os

app = FastAPI()

def get_best_model_path(models_dir="models/"):
    """Finds the model with the highest accuracy in the given directory."""
    best_model = None
    highest_accuracy = 0.0

    for filename in os.listdir(models_dir):
        if filename.endswith(".pkl"):
            try:
                accuracy = float(filename.split("_accuracy_")[1].replace(".pkl", ""))
                if accuracy > highest_accuracy:
                    highest_accuracy = accuracy
                    best_model = os.path.join(models_dir, filename)
            except (IndexError, ValueError):
                continue

    if not best_model:
        raise FileNotFoundError("No valid models found in the directory.")
    
    return best_model

MODEL_PATH = get_best_model_path()
print(f"Using best model: {MODEL_PATH}")

# Load the trained model
if os.path.exists(MODEL_PATH):
    with open(MODEL_PATH, "rb") as f:
        model = pickle.load(f)
else:
    raise FileNotFoundError(f"Model file not found at {MODEL_PATH}")

# Define input schema
class PredictionInput(BaseModel):
    num_critic_for_reviews: float
    duration: float
    director_facebook_likes: float
    actor_3_facebook_likes: float
    actor_1_facebook_likes: float
    gross: float
    num_voted_users: float
    cast_total_facebook_likes: float
    facenumber_in_poster: float
    num_user_for_reviews: float
    budget: float
    title_year: float
    actor_2_facebook_likes: float
    aspect_ratio: float
    movie_facebook_likes: float

@app.post("/predict")
def predict(input_data: PredictionInput):
    features = [[
        input_data.num_critic_for_reviews,
        input_data.duration,
        input_data.director_facebook_likes,
        input_data.actor_3_facebook_likes,
        input_data.actor_1_facebook_likes,
        input_data.gross,
        input_data.num_voted_users,
        input_data.cast_total_facebook_likes,
        input_data.facenumber_in_poster,
        input_data.num_user_for_reviews,
        input_data.budget,
        input_data.title_year,
        input_data.actor_2_facebook_likes,
        input_data.aspect_ratio,
        input_data.movie_facebook_likes
    ]]
    
    prediction = model.predict(features)
    
    return {"prediction": prediction.tolist()}

@app.get("/")
async def root():
    return {"message": "Hello World"}