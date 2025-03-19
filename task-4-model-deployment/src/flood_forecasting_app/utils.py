import openmeteo_requests
import requests_cache
import plotly.graph_objects as go
import pandas as pd
from retry_requests import retry


def fetch_meteo_data(start_date="2025-02-22" , end_date = "2025-03-03"  , fetch_target = False):

    try:
        # Setup the Open-Meteo API client with cache and retry on error
        cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
        retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
        openmeteo = openmeteo_requests.Client(session = retry_session)

        # If we are fetching the target, we need to use the flood API, otherwise we use the archive API
        if fetch_target:
            URL = "https://flood-api.open-meteo.com/v1/flood"
            params = {
                "latitude": 24.80,
                "longitude": 92.35,
                "daily": "river_discharge",
                "start_date": start_date,
                "end_date": end_date,
                "timezone": "UTC"
            }
        
        else :
            URL = "https://archive-api.open-meteo.com/v1/archive"
            params = {
                "latitude": 24.8692,
                "longitude": 92.3554,
                "start_date": start_date, #depends on model development team
                "end_date": end_date,
                "hourly": ["pressure_msl", "soil_moisture_100_to_255cm","temperature_2m"], #variables based on final dataset
                "daily": ["precipitation_sum", "wind_speed_10m_max", "wind_direction_10m_dominant", "et0_fao_evapotranspiration"],
                "wind_speed_unit": "ms"
            }

        responses = openmeteo.weather_api(URL, params=params)
        return responses
    
    except Exception as e:
        print("Error fetching data: ", e)
        return None
    

def get_features_from_response(responses):

    try:
        response = responses[0]
        
        ## Process hourly data. The order of variables needs to be the same as requested.
        hourly = response.Hourly()
        hourly_pressure_msl = hourly.Variables(0).ValuesAsNumpy()
        hourly_soil_moisture_100_to_255cm = hourly.Variables(1).ValuesAsNumpy()
        hourly_temperature_2m = hourly.Variables(2).ValuesAsNumpy()


        hourly_data = {"date": pd.date_range(
            start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
            end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
            freq = pd.Timedelta(seconds = hourly.Interval()),
            inclusive = "left"
        )}

        hourly_data["pressure_msl"] = hourly_pressure_msl
        hourly_data["soil_moisture_100_to_255cm"] = hourly_soil_moisture_100_to_255cm
        hourly_data["temperature_2m"] = hourly_temperature_2m


        hourly_dataframe = pd.DataFrame(data = hourly_data)

        ## Process daily data. The order of variables needs to be the same as requested.
        daily = response.Daily()
        daily_precipitation_sum = daily.Variables(0).ValuesAsNumpy()
        daily_wind_speed_10m_max = daily.Variables(1).ValuesAsNumpy()
        daily_wind_direction_10m_dominant = daily.Variables(2).ValuesAsNumpy()
        daily_et0_fao_evapotranspiration = daily.Variables(3).ValuesAsNumpy()

        daily_data = {"date": pd.date_range(
        start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
        end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
        freq = pd.Timedelta(seconds = daily.Interval()),
        inclusive = "left")}

        daily_data["precipitation_sum"] = daily_precipitation_sum
        daily_data["wind_speed_10m_max"] = daily_wind_speed_10m_max
        daily_data["wind_direction_10m_dominant"] = daily_wind_direction_10m_dominant
        daily_data["et0_fao_evapotranspiration"] = daily_et0_fao_evapotranspiration

        daily_dataframe = pd.DataFrame(data = daily_data)

        ## group the hourly dataset by day and calculate the daily average

        # Convert 'date' column to datetime format and extract only the date part
        hourly_dataframe["date"] = pd.to_datetime(hourly_dataframe["date"])
        hourly_dataframe["date"] = hourly_dataframe["date"].dt.date

        # Compute daily averages: gathers data by date, takes mean of gathered data if data are numeric( if not it doesn't take them into account)
        daily_avg = hourly_dataframe.groupby("date").mean(numeric_only=True).reset_index()

        # Format output, rounds pressure_msl to one decimal place and soil moisture to 3 decimal places
        daily_avg["pressure_msl"] = daily_avg["pressure_msl"].round(1)
        daily_avg["soil_moisture_100_to_255cm"] = daily_avg["soil_moisture_100_to_255cm"].round(3)

        ## Get the date for the daily dataset
        # Convert 'date' column to datetime format and extract only the date part
        daily_dataframe["date"] = pd.to_datetime(pd.to_datetime(daily_dataframe["date"]))
        daily_dataframe["date"] = daily_dataframe["date"].dt.date

        ## Merge the daily average and daily datasets
        merged_df = daily_dataframe.merge(daily_avg, on="date")
        return merged_df
    
    except Exception as e:
        print("Error processing the features: ", e)
        return None

def get_target_from_response(responses):

    try:
        response = responses[0]
        daily = response.Daily()
        daily_river_discharge = daily.Variables(0).ValuesAsNumpy()

        daily_data = {"date": pd.date_range(
            start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
            end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
            freq = pd.Timedelta(seconds = daily.Interval()),
            inclusive = "left"
        )}

        daily_data["river_discharge"] = daily_river_discharge

        daily_dataframe = pd.DataFrame(data = daily_data)

        # Convert 'date' column to datetime format and extract only the date part
        daily_dataframe["date"] = pd.to_datetime(pd.to_datetime(daily_dataframe["date"]))
        daily_dataframe["date"] = daily_dataframe["date"].dt.date

        return daily_dataframe
    
    except Exception as e:
        print("Error processing the river discharge data: ", e)
        return None
    
    
def merge_features_target(features, target):

    try:
        merged_df = features.merge(target, on="date")
        merged_df.set_index("date", inplace=True)
        return merged_df
    
    except Exception as e:
        print("Error merging the features and target: ", e)
        return None


def fetch_and_process_data(start_date="2025-02-22", end_date="2025-03-03"):

    try:
        # Fetch the data
        responses = fetch_meteo_data(start_date, end_date)
        if responses is None:
            return None

        # Process the features
        features = get_features_from_response(responses)
        if features is None:
            return None

        # Fetch the target
        target_responses = fetch_meteo_data(start_date, end_date, fetch_target = True)
        if target_responses is None:
            return None

        # Process the target
        target = get_target_from_response(target_responses)
        if target is None:
            return None

        # Merge the features and target
        merged_df = merge_features_target(features, target)
        if merged_df is None:
            return None

        return merged_df
    
    except Exception as e:
        print("Error fetching and processing the data: ", e)
        return None
    
def plot_and_disply_data_predictions(data ,discharge_col = "river_discharge", flood_col = "flood", proba_col = "proba"):
    """
    Plots river discharge levels and marks predicted flood days with red dots.
    
    Parameters:
    - data (pd.DataFrame): DataFrame containing the river discharge data.
    - date_col (str): Column name for the date.
    - discharge_col (str): Column name for the river discharge level.
    - flood_col (str): Column name indicating if a flood is predicted (1 for flood, 0 otherwise).
    - proba_col (str): Column name for the predicted flood probability.
    """
    
    try :
    # Create figure
        fig = go.Figure()
        
        # Add river discharge line
        fig.add_trace(go.Scatter(
            x=data.index, 
            y=data[discharge_col], 
            mode='lines',
            name='River Discharge Level',
            line=dict(color='blue')
        ))
        
        # Add flood prediction points
        flood_data = data[data[flood_col] == 1]
        fig.add_trace(go.Scatter(
            x=flood_data.index, 
            y=flood_data[discharge_col], 
            mode='markers',
            name='Predicted Flood',
            marker=dict(size=12, color='red', symbol='circle'),
            text=[f"Predicted Flood<br>Probability: {p:.2f}" for p in flood_data[proba_col]],
            hoverinfo='text'
        ))
        
        # Update layout
        fig.update_layout(
            title='River Discharge Levels with Flood Predictions',
            xaxis_title='Date',
            yaxis_title='Discharge Level',
            template='plotly_white'
        )
        return fig
    
    except Exception as e:
        print("Error plotting and displaying the data and predictions: ", e)
        return None
    

