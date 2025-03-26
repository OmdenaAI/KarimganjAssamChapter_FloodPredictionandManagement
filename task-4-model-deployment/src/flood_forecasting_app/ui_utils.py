import plotly.graph_objects as go
import pandas as pd


def get_feature_evolution(data):
    try:
        yesterday_date = data.index[-2]
        today_date = data.index[-1]

        yesterday_precipitation = data.loc[yesterday_date , "precipitation_sum (mm)"]
        yesterday_temperature = data.loc[yesterday_date , "temperature_2m_mean (°C)"]
        yesterday_river_discharge = data.loc[yesterday_date , "Longai_discharge (m³/s)"]
        yesterday_wind = data.loc[yesterday_date , "wind_speed_10m_max (m/s)"]

        today_precipitation = data.loc[today_date , "precipitation_sum (mm)"]
        today_temperature = data.loc[today_date , "temperature_2m_mean (°C)"]
        today_river_discharge = data.loc[today_date , "Longai_discharge (m³/s)"]
        today_wind = data.loc[today_date , "wind_speed_10m_max (m/s)"]

        precipitation_diff = (today_precipitation - yesterday_precipitation) / yesterday_precipitation * 100 if yesterday_precipitation != 0 else 0
        temperature_diff = (today_temperature - yesterday_temperature) / yesterday_temperature * 100 if yesterday_temperature != 0 else 0
        river_discharge_diff = (today_river_discharge - yesterday_river_discharge) / yesterday_river_discharge * 100 if yesterday_river_discharge != 0 else 0
        wind_diff = (today_wind - yesterday_wind) / yesterday_wind * 100 if yesterday_wind != 0 else 0

        return (today_precipitation,today_temperature,today_river_discharge,today_wind), (precipitation_diff, temperature_diff, river_discharge_diff, wind_diff)
    
    except Exception as e:
        print(f"An error occurred during feature evolution computing: {e}")
        return None, None


def plot_and_display_data_predictions(
    data, discharge_col="Longai_discharge (m³/s)", predicted_discharge_col="predicted_discharge", 
    flood_col="flood", proba_col="proba"
):
    """
    Plots river discharge levels, marks predicted flood days with red dots, 
    and includes predicted discharge with a dashed green transition line.

    Parameters:
    - data (pd.DataFrame): DataFrame containing river discharge data.
    - discharge_col (str): Column name for the river discharge level.
    - predicted_discharge_col (str): Column name for predicted discharge.
    - flood_col (str): Column name indicating if a flood is predicted (1 for flood, 0 otherwise).
    - proba_col (str): Column name for the predicted flood probability.
    """

    try:
        data.set_index('date', inplace=True)
        fig = go.Figure()
        
        # Plot known river discharge levels
        fig.add_trace(go.Scatter(
            x=data.index, 
            y=data[discharge_col], 
            mode='lines',
            name='River Discharge Level',
            line=dict(color='blue')
        ))

        # Add predicted discharge with a dashed green line from the last known value
        last_date = data.index[-1]  # Last date in the dataset
        if predicted_discharge_col in data.columns and not data[predicted_discharge_col].isna().all():
            next_date = last_date + pd.Timedelta(days=1)  # Next day's timestamp

            fig.add_trace(go.Scatter(
                x=[last_date, next_date], 
                y=[data.loc[last_date, discharge_col], data.loc[last_date, predicted_discharge_col]],
                mode='lines+markers',
                name='Predicted Discharge',
                line=dict(color='green', dash='dash'),
                marker=dict(size=8, color='green', symbol='circle')
            ))

        # Shift flood data to next date
        flood_data = data[data[flood_col] == 1].copy()
        flood_data.index += pd.Timedelta(days=1)  # Move flood predictions to the next day

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
        print("Error plotting and displaying the data and predictions:", e)
        return None
