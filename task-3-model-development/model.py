import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from xgboost import XGBRegressor, XGBClassifier
from sklearn.model_selection import train_test_split, RandomizedSearchCV
from sklearn.metrics import accuracy_score, precision_score, recall_score, roc_auc_score, mean_squared_error, r2_score, confusion_matrix, classification_report

# Load the data
df = pd.read_csv("AllData_B4_EDA.csv")
df["Date"] = pd.to_datetime(df["Date"])
df.dropna(inplace=True)

# Feature Engineering
df['month'] = df['Date'].dt.month
df['season'] = df['month'] % 12 // 3

df['rain_last_3_days'] = df['rain_sum (mm)'].rolling(window=3, min_periods=1).sum()
df['rain_last_7_days'] = df['rain_sum (mm)'].rolling(window=7, min_periods=1).sum()
df['Longai_discharge_last_3_days'] = df['Longai_discharge (m³/s)'].rolling(window=3, min_periods=1).sum()
df['Kushi_discharge_last_3_days'] = df['Kushi_discharge (m³/s)'].rolling(window=3, min_periods=1).sum()
df['Singla_discharge_last_3_days'] = df['Singla_discharge (m³/s)'].rolling(window=3, min_periods=1).sum()
df['Longai_discharge_last_7_days'] = df['Longai_discharge (m³/s)'].rolling(window=7, min_periods=1).sum()
df['Kushi_discharge_last_7_days'] = df['Kushi_discharge (m³/s)'].rolling(window=7, min_periods=1).sum()
df['Singla_discharge_last_7_days'] = df['Singla_discharge (m³/s)'].rolling(window=7, min_periods=1).sum()
df['soil_moisture_trend'] = df['soil_moisture_100_to_255cm (m³/m³)'].rolling(window=5, min_periods=1).mean()
df['rain_soil_interaction'] = df['rain_sum (mm)'] * df['soil_moisture_100_to_255cm (m³/m³)']
df['rivers_interaction'] = df['Longai_discharge (m³/s)'] * df['Kushi_discharge (m³/s)'] * df['Singla_discharge (m³/s)']

# Regression targets
y_rain = df["rain_sum (mm)"]
y_discharge = df["Longai_discharge (m³/s)"]
y_flood = df["flooded"]

# Chronological split
split_train = int(len(df) * 0.7)
split_val = int(len(df) * 0.85)

# Regression training
features = [
    "rain_sum (mm)", "Longai_discharge (m³/s)",
    "temperature_2m_max (°C)", "temperature_2m_min (°C)",
    "soil_moisture_0_to_7cm (m³/m³)", "soil_moisture_7_to_28cm (m³/m³)",
    "soil_moisture_28_to_100cm (m³/m³)", "soil_moisture_100_to_255cm (m³/m³)",
    "rain_last_7_days", "Longai_discharge_last_7_days",
    "soil_moisture_trend", "rain_soil_interaction", "rivers_interaction",
    "month", "season"
]

X_train_reg = df[features].iloc[:split_train]
y_rain_train, y_discharge_train = y_rain.iloc[:split_train], y_discharge.iloc[:split_train]
X_val_reg = df[features].iloc[split_train:split_val]
y_rain_val, y_discharge_val = y_rain.iloc[split_train:split_val], y_discharge.iloc[split_train:split_val]
X_test_reg = df[features].iloc[split_val:]
y_rain_test, y_discharge_test = y_rain.iloc[split_val:], y_discharge.iloc[split_val:]

rain_model = XGBRegressor(n_estimators=100, learning_rate=0.1, random_state=42)
rain_model.fit(X_train_reg, y_rain_train)
y_rain_pred_val = rain_model.predict(X_val_reg)
y_rain_pred_test = rain_model.predict(X_test_reg)

discharge_model = XGBRegressor(n_estimators=100, learning_rate=0.1, random_state=42)
discharge_model.fit(X_train_reg, y_discharge_train)
y_discharge_pred_val = discharge_model.predict(X_val_reg)
y_discharge_pred_test = discharge_model.predict(X_test_reg)

# Merge predictions into df
df['predicted_rain'] = np.nan
df['predicted_discharge'] = np.nan
df.loc[split_val:, 'predicted_rain'] = y_rain_pred_test
df.loc[split_val:, 'predicted_discharge'] = y_discharge_pred_test

# Flood features including predictions
flood_features = [
    'temperature_2m_max (°C)', 'temperature_2m_min (°C)', 'temperature_2m_mean (°C)', 'rain_sum (mm)',
    'precipitation_hours (h)', 'wind_speed_10m_max (m/s)', 'wind_gusts_10m_max (m/s)',
    'wind_direction_10m_dominant (°)', 'et0_fao_evapotranspiration (mm)', 'unknown_discharge (m³/s)',
    'Kushi_discharge (m³/s)', 'Longai_discharge (m³/s)', 'Singla_discharge (m³/s)',
    'pressure_msl (hPa)', 'soil_moisture_0_to_7cm (m³/m³)', 'soil_moisture_7_to_28cm (m³/m³)',
    'soil_moisture_28_to_100cm (m³/m³)', 'soil_moisture_100_to_255cm (m³/m³)', 'month', 'season',
    'rain_last_3_days', 'rain_last_7_days', 'Longai_discharge_last_3_days', 'Kushi_discharge_last_3_days',
    'Singla_discharge_last_3_days', 'Longai_discharge_last_7_days', 'Kushi_discharge_last_7_days',
    'Singla_discharge_last_7_days', 'soil_moisture_trend', 'rain_soil_interaction', 'rivers_interaction',
    'predicted_rain', 'predicted_discharge'
]

# Flood model split
X_train_flood = df[flood_features].iloc[:split_train]
y_train_flood = y_flood.iloc[:split_train]
X_val_flood = df[flood_features].iloc[split_train:split_val]
y_val_flood = y_flood.iloc[split_train:split_val]
X_test_flood = df[flood_features].iloc[split_val:]
y_test_flood = y_flood.iloc[split_val:]

flood_clf = XGBClassifier(n_estimators=100, learning_rate=0.1, max_depth=5, objective='multi:softmax', num_class=3)
flood_clf.fit(X_train_flood, y_train_flood)

print("Validation ML Flood Classifier Accuracy:", accuracy_score(y_val_flood, flood_clf.predict(X_val_flood)))

# Predict flood classes
flood_pred_test = flood_clf.predict(X_test_flood)

# Rule-based classification
def rule_based_class(row):
    rain = row['predicted_rain']
    discharge = row['predicted_discharge']
    avg_soil = np.mean([
        row['soil_moisture_0_to_7cm (m³/m³)'],
        row['soil_moisture_7_to_28cm (m³/m³)'],
        row['soil_moisture_28_to_100cm (m³/m³)'],
        row['soil_moisture_100_to_255cm (m³/m³)']
    ])

    if discharge <= 100 and rain <= 15:
        return 0  # No Flood
    elif (100 < discharge <= 200) or (15 < rain <= 50) or avg_soil > 0.35:
        return 1  # Mild Flood
    else:
        return 2  # Severe Flood

rule_classes = X_test_flood.apply(rule_based_class, axis=1)

# Evaluation
print("\nClassification Report for ML Prediction:")
print(classification_report(y_test_flood.reset_index(drop=True), flood_pred_test))

print("\nClassification Report for Rule-based Prediction:")
print(classification_report(y_test_flood.reset_index(drop=True), rule_classes))

plt.figure(figsize=(8, 5))
cm = confusion_matrix(y_test_flood.reset_index(drop=True), flood_pred_test)
sns.heatmap(cm, annot=True, fmt='d', cmap='Blues')
plt.title('Confusion Matrix - ML Flood Classifier')
plt.xlabel('Predicted')
plt.ylabel('Actual')
plt.show()

