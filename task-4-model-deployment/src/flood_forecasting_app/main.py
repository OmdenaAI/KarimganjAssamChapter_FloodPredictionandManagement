import streamlit as st
from modeling_utils import  predict_flood
from data_collection_utils import fetch_and_process_data 
from ui_utils import plot_and_display_data_predictions , get_feature_evolution
import datetime
from datetime import timedelta
import numpy as np


st.title("Flood Forecasting App 🌊")


@st.fragment(run_every="1d")
def get_input(start_date = "2025-02-22", end_date = str(datetime.date.today())):
    data = fetch_and_process_data(start_date, end_date)
    return data

@st.fragment(run_every="1d1m")
def plot_predictions(data):
    fig = plot_and_display_data_predictions(data)
    st.plotly_chart(fig)
   

col1 , col2 = st.columns(2)

today = datetime.date.today()

#Fetch the data
two_days_ago = (today - timedelta(days=2))
start_date = col1.date_input("Start date", value = two_days_ago)
start_date = start_date if start_date <= today else two_days_ago
# start_date = start_date.strftime("%Y-%m-%d")

end_date = col2.date_input("End date", value = today)
end_date = end_date if end_date <= today else today
# end_date = end_date.strftime("%Y-%m-%d")

data = get_input(start_date, end_date)

# Printing descriptive statistics about the river
col1, col2, col3 , col4 = st.columns(4)
today_features , features_evolution = get_feature_evolution(data)
today_precipitation , today_temperature , today_river_discharge , today_wind = today_features
precipitation_diff , temperature_diff , river_discharge_diff , wind_diff = features_evolution


col1.metric("River Discharge", f"{today_river_discharge:.2f} m³/s", "{:.2f}%".format(river_discharge_diff))
col2.metric("Precipitation", f"{today_precipitation:.2f} mm", "{:.2f}%" .format(precipitation_diff))
col3.metric("Temperature", f"{today_temperature:.2f} °C", "{:.2f}%".format(temperature_diff))
col4.metric("Wind", f"{today_wind:.2f} mph", "{:.2f}%".format(wind_diff))



# Getting the model and making the prediction   
data = predict_flood(data)


# Displaying the prediction and the river discharge
plot_predictions(data)

# Display the flooded prediction
flooded = data[data["flood"] == 1]
if not flooded.empty:
    st.markdown(
    '<p>There is a <span style="color:red">high chance</span> of flood on the following days:</p>',
    unsafe_allow_html=True)

    st.write(flooded.index)
else:
    st.markdown(
    '</p>There is a <span style="color:green">low chance</span> of flood in the next few days.</p>',
    unsafe_allow_html=True)
    




