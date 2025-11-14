from airflow import DAG
from airflow.operators.python import PythonOperator
from requests import get
from pandas import DataFrame

API_KEY = "3bffb37f9aa74627a73212454251311"

url = f"http://api.weatherapi.com/v1/current.json?key={API_KEY}&q=Moscow"

def extract(url: str) -> dict:
    # request = get(url)
    # return request.json()
    return dict({'location': {'name': 'Moscow', 'region': 'Moscow City', 'country': 'Russia', 'lat': 55.7522, 'lon': 37.6156, 'tz_id': 'Europe/Moscow', 'localtime_epoch': 1763069490, 'localtime': '2025-11-14 00:31'}, 'current': {'last_updated_epoch': 1763069400, 'last_updated': '2025-11-14 00:30', 'temp_c': 4.2, 'temp_f': 39.6, 'is_day': 0, 'condition': {'text': 'Overcast', 'icon': '//cdn.weatherapi.com/weather/64x64/night/122.png', 'code': 1009}, 'wind_mph': 15.2, 'wind_kph': 24.5, 'wind_degree': 217, 'wind_dir': 'SW', 'pressure_mb': 1012.0, 'pressure_in': 29.88, 'precip_mm': 0.0, 'precip_in': 0.0, 'humidity': 93, 'cloud': 100, 'feelslike_c': -0.5, 'feelslike_f': 31.2, 'windchill_c': 1.0, 'windchill_f': 33.8, 'heatindex_c': 5.3, 'heatindex_f': 41.6, 'dewpoint_c': 1.4, 'dewpoint_f': 34.6, 'vis_km': 10.0, 'vis_miles': 6.0, 'uv': 0.0, 'gust_mph': 22.5, 'gust_kph': 36.3, 'short_rad': 0, 'diff_rad': 0, 'dni': 0, 'gti': 0}})


def transform(data: dict) -> DataFrame:
    location_keys = ['name', 'region', 'country', 'tz_id', 'localtime']
    current_keys = ['temp_c', 'wind_kph', 'humidity', 'feelslike_c']
    data_to_data_frame = {}
    for key in data.keys():
        if key == 'location':
            for k, v in data[key].items():
                if k in location_keys:
                    data_to_data_frame[k] = v
        if key == 'current':
            for k, v in data[key].items():
                if k in current_keys:
                    data_to_data_frame[k] = v
    df = DataFrame([data_to_data_frame])
    return df


def load(data: DataFrame) -> bool:
    data.to_csv("weather_data.csv", mode="a", header=False, index=False, encoding='utf-8', sep=';')
    return True

data = extract(url)
transformed_data = transform(data)
load(transformed_data)