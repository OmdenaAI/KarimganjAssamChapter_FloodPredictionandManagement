import streamlit as st
from modeling_utils import MockModel , predict_flood
from utils import fetch_and_process_data , plot_and_disply_data_predictions
import datetime
from datetime import timedelta
import numpy as np


st.title("Flood Forecasting App 🌊")

if "mean_precipitation" not in st.session_state:
    st.session_state.mean_precipitation = 0
    st.session_state.mean_temperature = 0
    st.session_state.mean_river_discharge = 0
    st.session_state.mean_wind = 0

@st.cache_resource
def get_model():
    return MockModel()

@st.fragment(run_every="1d")
def get_input(start_date = "2025-02-22", end_date = str(datetime.date.today())):
    data = fetch_and_process_data(start_date, end_date)
    return data

@st.fragment(run_every="1d1m")
def plot_predictions(data):
    fig = plot_and_disply_data_predictions(data)
    st.plotly_chart(fig)
   

col1 , col2 = st.columns(2)

#Fetch the data
two_days_ago = (datetime.date.today() - timedelta(days=2))
start_date = col1.date_input("Start date", value = two_days_ago)
start_date = start_date if start_date >= datetime.date(2025, 2, 22) else two_days_ago
# start_date = start_date.strftime("%Y-%m-%d")

today = datetime.date.today()
end_date = col2.date_input("End date", value = today)
end_date = end_date if end_date <= today else today
# end_date = end_date.strftime("%Y-%m-%d")

data = get_input(start_date, end_date)



# Printing descriptive statistics about the river
col1, col2, col3 , col4 = st.columns(4)
mean_precipitation = data["precipitation_sum"].mean()
mean_temperature = data["temperature_2m"].mean()
mean_river_discharge = data["river_discharge"].mean()
mean_wind= data["wind_speed_10m_max"].mean()

mean_precipitation_diff = (mean_precipitation - st.session_state.mean_precipitation) / st.session_state.mean_precipitation * 100 if st.session_state.mean_precipitation != 0 else 0
mean_temperature_diff = (mean_temperature - st.session_state.mean_temperature) / st.session_state.mean_temperature * 100 if st.session_state.mean_temperature != 0 else 0
mean_river_discharge_diff = (mean_river_discharge - st.session_state.mean_river_discharge) / st.session_state.mean_river_discharge * 100 if st.session_state.mean_river_discharge != 0 else 0
mean_wind_diff = (mean_wind - st.session_state.mean_wind) / st.session_state.mean_wind * 100 if st.session_state.mean_wind != 0 else 0

col1.metric("River Discharge", f"{mean_river_discharge:.2f} m³/s", "{:.2f}%".format(mean_river_discharge_diff))
col2.metric("Precipitation", f"{mean_precipitation:.2f} mm", "{:.2f}%" .format(mean_precipitation_diff))
col3.metric("Temperature", f"{mean_temperature:.2f} °F", "{:.2f}%".format(mean_temperature_diff))
col4.metric("Wind", f"{mean_wind:.2f} mph", "{:.2f}%".format(mean_wind_diff))

st.session_state.mean_precipitation = mean_precipitation
st.session_state.mean_temperature = mean_temperature
st.session_state.mean_river_discharge = mean_river_discharge
st.session_state.mean_wind = mean_wind

# Getting the model and making the prediction   
model = get_model()
data = predict_flood(data, model)

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
    




