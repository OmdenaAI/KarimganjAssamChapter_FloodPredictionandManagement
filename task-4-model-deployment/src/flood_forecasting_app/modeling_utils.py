import joblib
import pandas as pd
import numpy as np
from datetime import timedelta
import random

# Here I defined class to act mock model, it's kind of baseline(random predicting 0 or 1)
class MockModel:
    def predict(self, weather_data):
        # Return a list like real ML models
        return [random.choice([0, 1]) for i in range(len(weather_data))] 
    
    def predict_proba(self ,weather_data):
        return [random.random() for i in range(len(weather_data))]
         


def predict_flood(data , model):
    last_date = data.index[-1]
    last_obs = data.loc[last_date].to_numpy().reshape(1, -1)

    predicted_flood = model.predict(last_obs)
    predicted_proba = model.predict_proba(last_obs)
    n = len(predicted_flood)

    # Create a DataFrame for future predictions
    future_dates = [last_date + timedelta(days=i+1) for i in range(n)]
    future_df = pd.DataFrame({
        'date': future_dates,
        'flood': predicted_flood,
        'proba': predicted_proba,
        'river_discharge': np.random.randint(40, 50, n),
    }).set_index('date')

    data[['flood', 'proba']] = None  
    data = pd.concat([data, future_df])
    return data