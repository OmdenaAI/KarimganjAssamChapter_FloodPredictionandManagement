import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry


def fetch_meteo_data(start_date="2025-02-22" , end_date = "2025-03-03"  , fetch_target = False, coords = (24.80, 92.35)):

    try:
        # Setup the Open-Meteo API client with cache and retry on error
        cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
        retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
        openmeteo = openmeteo_requests.Client(session = retry_session)

        # If we are fetching the target, we need to use the flood API, otherwise we use the archive API
        if fetch_target:
            URL = "https://flood-api.open-meteo.com/v1/flood"
            params = {
                "latitude": coords[0],
                "longitude": coords[1],
                "daily": "river_discharge",
                "start_date": start_date,
                "end_date": end_date,
                "timezone": "UTC"
            }
        
        else :
            URL = "https://archive-api.open-meteo.com/v1/archive"
            params = {
                "latitude": coords[0],
                "longitude": coords[1],
                "start_date": start_date, #depends on model development team
                "end_date": end_date,
                "hourly": ["pressure_msl","soil_moisture_0_to_7cm","soil_moisture_7_to_28cm",
                           "soil_moisture_28_to_100cm", "soil_moisture_100_to_255cm" ], #variables based on final dataset
                "daily": ["precipitation_sum", "wind_speed_10m_max",
                           "wind_direction_10m_dominant", "et0_fao_evapotranspiration",
                           "wind_gusts_10m_max","temperature_2m_max","temperature_2m_min" ,
                           "temperature_2m_mean","rain_sum"],
                "wind_speed_unit": "ms",
                "temperature_unit": "celsius"
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
        hourly_soil_moisture_0_to_7cm = hourly.Variables(1).ValuesAsNumpy()
        hourly_soil_moisture_7_to_28cm = hourly.Variables(2).ValuesAsNumpy()
        hourly_soil_moisture_28_to_100cm = hourly.Variables(3).ValuesAsNumpy()
        hourly_soil_moisture_100_to_255cm = hourly.Variables(4).ValuesAsNumpy()


        hourly_data = {"date": pd.date_range(
            start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
            end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
            freq = pd.Timedelta(seconds = hourly.Interval()),
            inclusive = "left"
        )}

        hourly_data["pressure_msl (hPa)"] = hourly_pressure_msl
        hourly_data["soil_moisture_0_to_7cm (m³/m³)"] = hourly_soil_moisture_0_to_7cm
        hourly_data["soil_moisture_7_to_28cm (m³/m³)"] = hourly_soil_moisture_7_to_28cm
        hourly_data["soil_moisture_28_to_100cm (m³/m³)"] = hourly_soil_moisture_28_to_100cm
        hourly_data["soil_moisture_100_to_255cm (m³/m³)"] = hourly_soil_moisture_100_to_255cm

        hourly_dataframe = pd.DataFrame(data = hourly_data)

        ## Process daily data. The order of variables needs to be the same as requested.
        daily = response.Daily()
        daily_precipitation_sum = daily.Variables(0).ValuesAsNumpy()
        daily_wind_speed_10m_max = daily.Variables(1).ValuesAsNumpy()
        daily_wind_direction_10m_dominant = daily.Variables(2).ValuesAsNumpy()
        daily_et0_fao_evapotranspiration = daily.Variables(3).ValuesAsNumpy()
        daily_wind_gusts_10m_max = daily.Variables(4).ValuesAsNumpy()
        daily_temperature_2m_max = daily.Variables(5).ValuesAsNumpy()
        daily_temperature_2m_min = daily.Variables(6).ValuesAsNumpy()
        daily_temperature_2m_mean = daily.Variables(7).ValuesAsNumpy()
        daily_rain_sum = daily.Variables(8).ValuesAsNumpy()

        daily_data = {"date": pd.date_range(
        start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
        end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
        freq = pd.Timedelta(seconds = daily.Interval()),
        inclusive = "left")}

        daily_data["precipitation_sum (mm)"] = daily_precipitation_sum
        daily_data["wind_speed_10m_max (m/s)"] = daily_wind_speed_10m_max
        daily_data["wind_direction_10m_dominant"] = daily_wind_direction_10m_dominant
        daily_data["et0_fao_evapotranspiration (mm)"] = daily_et0_fao_evapotranspiration
        daily_data["wind_gusts_10m_max (m/s)"] = daily_wind_gusts_10m_max
        daily_data["temperature_2m_max (°C)"] = daily_temperature_2m_max
        daily_data["temperature_2m_min (°C)"] = daily_temperature_2m_min
        daily_data["temperature_2m_mean (°C)"] = daily_temperature_2m_mean
        daily_data["rain_sum (mm)"] = daily_rain_sum

        daily_dataframe = pd.DataFrame(data = daily_data)

        ## group the hourly dataset by day and calculate the daily average

        # Convert 'date' column to datetime format and extract only the date part
        hourly_dataframe["date"] = pd.to_datetime(hourly_dataframe["date"])
        hourly_dataframe["date"] = hourly_dataframe["date"].dt.date

        # Compute daily averages: gathers data by date, takes mean of gathered data if data are numeric( if not it doesn't take them into account)
        daily_avg = hourly_dataframe.groupby("date").mean(numeric_only=True).reset_index()

        # Format output, rounds pressure_msl to one decimal place and soil moisture to 3 decimal places
        daily_avg["pressure_msl (hPa)"] = daily_avg["pressure_msl (hPa)"].round(1)
        daily_avg["soil_moisture_0_to_7cm (m³/m³)"] = daily_avg["soil_moisture_0_to_7cm (m³/m³)"].round(3)
        daily_avg["soil_moisture_7_to_28cm (m³/m³)"] = daily_avg["soil_moisture_7_to_28cm (m³/m³)"].round(3)
        daily_avg["soil_moisture_28_to_100cm (m³/m³)"] = daily_avg["soil_moisture_28_to_100cm (m³/m³)"].round(3)
        daily_avg["soil_moisture_100_to_255cm (m³/m³)"] = daily_avg["soil_moisture_100_to_255cm (m³/m³)"].round(3)

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

def get_target_from_response(responses , name = "Longai_discharge (m³/s)"):

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

        daily_data[name] = daily_river_discharge

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
        longai_target_responses = fetch_meteo_data(start_date, end_date, fetch_target = True)
        if longai_target_responses is None:
            return None
        
        kushiyara_target_responses = fetch_meteo_data(start_date, end_date, fetch_target = True , coords = (24.6266, 91.7782))
        if kushiyara_target_responses is None:
            return None
        
        singla_target_responses = fetch_meteo_data(start_date, end_date, fetch_target = True , coords=(24.68216,92.4457))
        if singla_target_responses is None:
            return None
        
        unknown_target_responses = fetch_meteo_data(start_date, end_date, fetch_target = True , coords=(24.85,92.32))
        if unknown_target_responses is None:
            return None

        # Process the target
        longai_river_discharge_data = get_target_from_response(longai_target_responses)
        if longai_river_discharge_data is None:
            return None
        
        kushiyara_river_discharge_data = get_target_from_response(kushiyara_target_responses , name = "Kushi_discharge (m³/s)")
        if kushiyara_river_discharge_data is None:
            return None
        
        singla_river_discharge_data = get_target_from_response(singla_target_responses , name = "Singla_discharge (m³/s)")
        if singla_river_discharge_data is None:
            return None
        
        unknown_river_discharge_data = get_target_from_response(unknown_target_responses , name = "unknown_discharge (m³/s)")
        if unknown_river_discharge_data is None:
            return None

        # Merge the features and target
        merged_df = merge_features_target(features, longai_river_discharge_data)
        merged_df = merge_features_target(merged_df, kushiyara_river_discharge_data)
        merged_df = merge_features_target(merged_df, singla_river_discharge_data)
        merged_df = merge_features_target(merged_df, unknown_river_discharge_data)
        if merged_df is None:
            return None
        
        merged_df.interpolate(method="linear", inplace=True)
        return merged_df
    
    except Exception as e:
        print("Error fetching and processing the data: ", e)
        return None
    
