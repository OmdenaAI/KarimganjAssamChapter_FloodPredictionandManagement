import joblib
import pandas as pd
import numpy as np
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent


# Helper function to load the models
def load_model (model_path):
    return joblib.load(model_path)

# Load the models	
discharge_model = load_model(BASE_DIR/"models/discharge_model.joblib")
rain_model = load_model(BASE_DIR/"models/rain_model.joblib")
flood_model = load_model(BASE_DIR/"models/flood_clf_rfe.joblib")


# Helper function to preprocess the data
def preprocess_data(data):
    try:
        data["date"] = pd.to_datetime(data["date"])

        # Feature Engineering
        data['month'] = data['date'].dt.month
        data['season'] = data['month'] % 12 // 3

        data['rain_last_3_days'] = data['rain_sum (mm)'].rolling(window=3, min_periods=1).sum()
        data['rain_last_7_days'] = data['rain_sum (mm)'].rolling(window=7, min_periods=1).sum()
        data['Longai_discharge_last_3_days'] = data['Longai_discharge (m³/s)'].rolling(window=3, min_periods=1).sum()
        data['Kushi_discharge_last_3_days'] = data['Kushi_discharge (m³/s)'].rolling(window=3, min_periods=1).sum()
        data['Singla_discharge_last_3_days'] = data['Singla_discharge (m³/s)'].rolling(window=3, min_periods=1).sum()
        data['Longai_discharge_last_7_days'] = data['Longai_discharge (m³/s)'].rolling(window=7, min_periods=1).sum()
        data['Kushi_discharge_last_7_days'] = data['Kushi_discharge (m³/s)'].rolling(window=7, min_periods=1).sum()
        data['Singla_discharge_last_7_days'] = data['Singla_discharge (m³/s)'].rolling(window=7, min_periods=1).sum()
        data['soil_moisture_trend'] = data['soil_moisture_100_to_255cm (m³/m³)'].rolling(window=5, min_periods=1).mean()
        data['rain_soil_interaction'] = data['rain_sum (mm)'] * data['soil_moisture_100_to_255cm (m³/m³)']
        data['rivers_interaction'] = data['Longai_discharge (m³/s)'] * data['Kushi_discharge (m³/s)'] * data['Singla_discharge (m³/s)']
    
    except Exception as e:
        print(f"An error occurred during preprocessing: {e}")
        
    return data.copy()
    

         

## Predict Function
regression_features = [
    "rain_sum (mm)", "Longai_discharge (m³/s)",
    "temperature_2m_max (°C)", "temperature_2m_min (°C)",
    "soil_moisture_0_to_7cm (m³/m³)", "soil_moisture_7_to_28cm (m³/m³)",
    "soil_moisture_28_to_100cm (m³/m³)", "soil_moisture_100_to_255cm (m³/m³)",
    "rain_last_7_days", "Longai_discharge_last_7_days",
    "soil_moisture_trend", "rain_soil_interaction", "rivers_interaction",
    "month", "season"
]

flood_features = ['temperature_2m_min (°C)', 'temperature_2m_mean (°C)',
       'unknown_discharge (m³/s)', 'Kushi_discharge (m³/s)',
       'Longai_discharge (m³/s)', 'Singla_discharge (m³/s)',
       'pressure_msl (hPa)', 'soil_moisture_0_to_7cm (m³/m³)',
       'soil_moisture_7_to_28cm (m³/m³)', 'soil_moisture_28_to_100cm (m³/m³)',
       'soil_moisture_100_to_255cm (m³/m³)', 'month',
       'Longai_discharge_last_3_days', 'Kushi_discharge_last_3_days',
       'Singla_discharge_last_3_days', 'Longai_discharge_last_7_days',
       'Kushi_discharge_last_7_days', 'Singla_discharge_last_7_days',
       'soil_moisture_trend', 'rivers_interaction']


def predict_flood(data):

    try:  
        data = preprocess_data(data)

        # Predict the rain and discharge
        last_date = data.index[-1]
        last_obs_regr = data.loc[last_date, regression_features].to_numpy().reshape(1, -1)
        predicted_rain = rain_model.predict(last_obs_regr)[0]
        predicted_discharge = discharge_model.predict(last_obs_regr)[0]

        # Ensure columns exist and update values
        for col, value in [('predicted_rain', predicted_rain), ('predicted_discharge', predicted_discharge)]:
            if col not in data.columns:
                data[col] = np.nan  # Initialize column with NaN
            data.at[last_date, col] = value

        # Predict the flood
        last_obs_flood = data.loc[last_date, flood_features].to_numpy().reshape(1, -1)
        predicted_flood = flood_model.predict(last_obs_flood)[0]
        predicted_flood_proba = flood_model.predict_proba(last_obs_flood)[:, 1][0]  # Extract probability for class 1

        # Ensure flood-related columns exist and update values
        for col, value in [('flood', predicted_flood), ('proba', predicted_flood_proba)]:
            if col not in data.columns:
                data[col] = np.nan
            data.at[last_date, col] = value

        return data.copy()
    
    except Exception as e:
        print(f"An error occurred during prediction: {e}")
        return data.copy()