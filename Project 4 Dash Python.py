from cassandra.cluster import Cluster
import pandas as pd
import plotly.graph_objects as go
import ipywidgets as widgets
from IPython.display import display
import pymongo
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import os
import geopy
from geopy.geocoders import Nominatim

print(os.getcwd()) #Make sure MongoDB.txt is located in current working directory

USR, PWD = open('MongoDB.txt').read().splitlines()

uri = "mongodb+srv://ingebrigtkjaereng:" + PWD + "@ind320.sa7i4.mongodb.net/"

client = MongoClient(uri, server_api=ServerApi('1'))

try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(f"Error connecting to MongoDB: {e}")

database = client['example']
municipality_collection = database['municipalities']
gas_collection = database['gasprices']

import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

# Configure environment variables
os.environ["JAVA_HOME"] = "C:/Program Files/Microsoft/jdk-21.0.4.7-hotspot"
os.environ["PYSPARK_PYTHON"] = "C:/Users/chels/anaconda3/envs/pyspark2/python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:/Users/chels/anaconda3/envs/pyspark2/python.exe"
os.environ["HADOOP_HOME"] = "C:/Hadoop/hadoop-3.3.1"
os.environ["PYSPARK_HADOOP_VERSION"] = "without"

# Initialize Spark session with Cassandra connector
from pyspark import SparkConf

conf = SparkConf() \
    .set("spark.pyspark.python", "C:/Users/chels/anaconda3/envs/pyspark2/python.exe") \
    .set("spark.pyspark.driver.python", "C:/Users/chels/anaconda3/envs/pyspark2/python.exe") \
    .set("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1") \
    .set("spark.cassandra.connection.host", "localhost") \
    .set("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
    .set("spark.sql.catalog.mycatalog", "com.datastax.spark.connector.datasource.CassandraCatalog") \
    .set("spark.cassandra.connection.port", "9042")

spark = SparkSession.builder \
    .config(conf=conf) \
    .appName('CassandraDataFetcher') \
    .getOrCreate()

# Load data from Cassandra into Spark DataFrames
production_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "my_project_keyspace") \
    .option("table", "production") \
    .load()

consumption_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "my_project_keyspace") \
    .option("table", "consumption") \
    .load()

prodcon_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "my_project_keyspace") \
    .option("table", "prodcon") \
    .load()

weather_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "my_project_keyspace") \
    .option("table", "weather_data_2") \
    .load()

from pyspark.sql.functions import col
from pyspark.sql.functions import to_timestamp
# Set timezone for Spark
spark.conf.set("spark.sql.session.timeZone", "UTC")

# Filter out null or invalid timestamps
weather_df = weather_df.filter(col("timestamp").isNotNull())
# Convert string to timestamp in Spark
weather_df = weather_df.withColumn("timestamp", to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))

# Filter data for timestamps in 2022
weather_df = weather_df.filter((col("timestamp") >= "2022-01-01 00:00:00") & 
                                (col("timestamp") <= "2022-12-31 23:59:59"))

#Converted to Pandas DataFrames for Pivoting and GroupBy operations
production_pd = production_df.toPandas()
production_pd = production_pd.interpolate(method='linear') 
production_pd = production_pd.fillna(method='ffill')
production_pd = production_pd.fillna(method='bfill')
consumption_pd = consumption_df.toPandas()
consumption_pd = consumption_pd.interpolate(method='linear') 
consumption_pd = consumption_pd.fillna(method='ffill')
consumption_pd = consumption_pd.fillna(method='bfill')
prodcon_pd = prodcon_df.toPandas()
prodcon_pd = prodcon_pd.interpolate(method='linear') 
prodcon_pd = prodcon_pd.fillna(method='ffill')
prodcon_pd = prodcon_pd.fillna(method='bfill')
weather_pd = weather_df.toPandas()
weather_pd["timestamp"] = pd.to_datetime(weather_pd["timestamp"])

municipality_cursor = municipality_collection.find({}, {
    '_id': 0,  #Exclude MongoDB's internal ID
    'LAU-1 code 1': 1,
    'Municipality': 1,
    'Administrative Center': 1,
    'Total Area (km²)': 1,
    'Population (2012-01-01)': 1,
    'Region': 1,
    'Latitude': 1,
    'Longitude': 1,
    'PowerRegion': 1
}).limit(98)

# Convert to Pandas DataFrame
municipality_df = pd.DataFrame(list(municipality_cursor))

print(municipality_df.head())

municipality_map = {
    "101": "København",
    "147": "Frederiksberg",
    "151": "Ballerup",
    "153": "Brøndby",
    "155": "Dragør",
    "157": "Gentofte",
    "159": "Gladsaxe",
    "161": "Glostrup",
    "163": "Herlev",
    "165": "Albertslund",
    "167": "Hvidovre",
    "169": "Høje-Taastrup",
    "173": "Lyngby-Taarbæk",
    "175": "Rødovre",
    "183": "Ishøj",
    "185": "Tårnby",
    "187": "Vallensbæk",
    "190": "Furesø",
    "201": "Allerød",
    "210": "Fredensborg",
    "217": "Helsingør",
    "219": "Hillerød",
    "223": "Hørsholm",
    "230": "Rudersdal",
    "240": "Egedal",
    "250": "Frederikssund",
    "253": "Greve",
    "259": "Køge",
    "260": "Halsnæs",
    "265": "Roskilde",
    "269": "Solrød",
    "270": "Gribskov",
    "306": "Odsherred",
    "316": "Holbæk",
    "320": "Faxe",
    "326": "Kalundborg",
    "329": "Ringsted",
    "330": "Slagelse",
    "336": "Stevns",
    "340": "Sorø",
    "350": "Lejre",
    "360": "Lolland",
    "370": "Næstved",
    "376": "Guldborgsund",
    "390": "Vordingborg",
    "400": "Bornholm",
    "410": "Middelfart",
    "411": "Christiansø",
    "420": "Assens",
    "430": "Faaborg-Midtfyn",
    "440": "Kerteminde",
    "450": "Nyborg",
    "461": "Odense",
    "479": "Svendborg",
    "480": "Nordfyns",
    "482": "Langeland",
    "492": "Ærø",
    "510": "Haderslev",
    "530": "Billund",
    "540": "Sønderborg",
    "550": "Tønder",
    "561": "Esbjerg",
    "563": "Fanø",
    "573": "Varde",
    "575": "Vejen",
    "580": "Aabenraa",
    "607": "Fredericia",
    "615": "Horsens",
    "621": "Kolding",
    "630": "Vejle",
    "657": "Herning",
    "661": "Holstebro",
    "665": "Lemvig",
    "671": "Struer",
    "706": "Syddjurs",
    "707": "Norddjurs",
    "710": "Favrskov",
    "727": "Odder",
    "730": "Randers",
    "740": "Silkeborg",
    "741": "Samsø",
    "746": "Skanderborg",
    "751": "Aarhus",
    "756": "Ikast-Brande",
    "760": "Ringkøbing-Skjern",
    "766": "Hedensted",
    "773": "Morsø",
    "779": "Skive",
    "787": "Thisted",
    "791": "Viborg",
    "810": "Brønderslev",
    "813": "Frederikshavn",
    "820": "Vesthimmerlands",
    "825": "Læsø",
    "840": "Rebild",
    "846": "Mariagerfjord",
    "849": "Jammerbugt",
    "851": "Aalborg",
    "860": "Hjørring"
}

production_pd['hour_utc'] = pd.to_datetime(production_pd['hour_utc'], errors='coerce')
consumption_pd['hour_utc'] = pd.to_datetime(consumption_pd['hour_utc'], errors='coerce')
prodcon_pd['hour_utc'] = pd.to_datetime(prodcon_pd['hour_utc'], errors='coerce')

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
import geopandas as gpd
import plotly.graph_objs as go
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import mean

#Adding distance_to_station_km to the municipality hovertext:

# 1. Extract the unique 'distance_to_station_km' for each municipality from weather_pd
# Since 'distance_to_station_km' is the same for all records per municipality, we can use groupby and get the first occurrence

# Clean up and merge the data (this step would have already been done earlier)

# Merge the cleaned weather data with municipality_df, assuming 'municipality_df' and 'weather_pd' are available
import pandas as pd
from pyspark.sql import Window
from pyspark.sql.functions import col

# Example: Assuming you already have municipality_df and weather_pd loaded

# Drop rows where 'distance_to_station_km' is NaN
weather_pd_cleaned = weather_pd.dropna(subset=['distance_to_station_km'])

# Step 2: Group by 'Municipality' and get the first non-NaN 'distance_to_station_km' for each municipality
distance_df = weather_pd_cleaned.groupby('municipality')['distance_to_station_km'].first().reset_index()

# Standardize column names for consistency and avoid conflicts
distance_df.columns = distance_df.columns.str.strip().str.lower()
municipality_df.columns = municipality_df.columns.str.strip().str.lower()

# Step 3: Merge 'municipality_df' with 'distance_df' on 'municipality'
# Avoid column conflicts by assigning suffixes
municipality_df = pd.merge(municipality_df, distance_df, on='municipality', how='left', suffixes=('', '_from_weather'))

# Step 4: Ensure correct distance column is used and clean up after merge
# Keep only the original 'distance_to_station_km' column, or fallback to '_from_weather' if necessary
if 'distance_to_station_km' not in municipality_df.columns:
    municipality_df.rename(columns={'distance_to_station_km_from_weather': 'distance_to_station_km'}, inplace=True)
else:
    municipality_df.drop(columns=['distance_to_station_km_from_weather'], inplace=True, errors='ignore')

# Step 5: Add hover text
municipality_df['hover_text'] = municipality_df.apply(
    lambda row: (
        f"{row['municipality']}<br>"
        f"Latitude: {row['latitude']}<br>"
        f"Longitude: {row['longitude']}<br>"
        f"PowerRegion: {row['powerregion']}<br>"
        f"LAU-1 Code: {row['lau-1 code 1']}<br>"
        f"Administrative Center: {row['administrative center']}<br>"
        f"Total Area (km²): {row['total area (km²)']}<br>"
        f"Population (2012-01-01): {row['population (2012-01-01)']}<br>"
        f"Region: {row['region']}<br>"
        # Include Distance to Station only if it's not NaN
        + (f"Distance to Station (km): {row['distance_to_station_km']}" if pd.notna(row['distance_to_station_km']) else "")
    ),
    axis=1
)

print(municipality_df.head())

def get_import_export_locations(filtered_import_export_data): 
    # Summarize import/export data
    exchange_columns = ['exchangegb_mwh', 'exchangege_mwh', 'exchangenl_mwh', 'exchangeno_mwh']
    if filtered_import_export_data.empty:
        return []
    exchanges = filtered_import_export_data[exchange_columns].sum()
    print("Exchanges Sum (in get_import_export_locations):", exchanges)
    print("Exchange Columns Used:", exchange_columns)
    print("Filtered Data Provided to Locations:", filtered_import_export_data.head())
    locations = []
    for col in exchange_columns:
        if col in exchanges and pd.notna(exchanges[col]) and exchanges[col] != 0:
            country_info = neighboring_countries.get(col, {})
            if country_info:  # Ensure country info exists
                locations.append({
                    'name': country_info['name'],
                    'lat': country_info['lat'],
                    'lon': country_info['lon'],
                    'value': exchanges[col]
                })
    print("Generated Locations:", locations)
    return locations

selected_date = pd.Timestamp('2022-01-01') + pd.Timedelta(days=1)
filtered_import_export_data = prodcon_pd[
        (prodcon_pd['price_area'].isin(['DK1', 'DK2'])) & 
        (prodcon_pd['hour_utc'].dt.date == selected_date.date())  # Direct datetime comparison
    ]
missing_exchangegb = prodcon_pd['exchangegb_mwh'].isna().sum()
print(f"Missing values in 'exchangegb_mwh': {missing_exchangegb}")

neighboring_countries = {
    'exchangegb_mwh': {'name': 'Great Britain', 'lat': 51.5074, 'lon': -0.1278},
    'exchangenl_mwh': {'name': 'Netherlands', 'lat': 52.3676, 'lon': 4.9041},
    'exchangege_mwh': {'name': 'Germany', 'lat': 52.52, 'lon': 13.4050},
    'exchangeno_mwh': {'name': 'Norway', 'lat': 59.9139, 'lon': 10.7522}}

example = get_import_export_locations(filtered_import_export_data)
# Initialize Spark session
# Verify the changes
print(prodcon_pd[['exchangegb_mwh', 'exchangenl_mwh', 'exchangeno_mwh', 'exchangege_mwh']].head())
foreign_country_coordinates = {
    "Great Britain": {"lon": -0.1278, "lat": 51.5074},  # Example: London coordinates
    "Germany": {"lon": 10.4515, "lat": 51.1657},        # Example: Central Germany
    "Netherlands": {"lon": 5.2913, "lat": 52.1326},     # Example: Central Netherlands
    "Norway": {"lon": 8.4689, "lat": 60.4720}           # Example: Central Norway
}

import json

print(weather_pd['weather_data'].head())
print(type(weather_pd['weather_data'].iloc[0]))
weather_pd['municipality'] = weather_pd['municipality'].replace("Copenhagen", "København")
print(weather_pd['municipality'].unique())

from plotly.subplots import make_subplots
import plotly.graph_objects as go
from scipy.fftpack import dct, idct
import writer as wf
from writer.core import WriterState
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import pandas as pd
from ast import literal_eval
import geopandas as gpd
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import mean
import numpy as np

valid_municipalities = ["København", "Aarhus", "Aalborg", "Odense", "Esbjerg"]

def safe_load(json_string):
    try:
        return json.loads(json_string)
    except (TypeError, ValueError):
        return {}

def update_municipality(row):
    try:
        weather_json = json.loads(row['weather_data'])
        if weather_json.get('municipality') == 'Copenhagen':
            weather_json['municipality'] = 'København'
        return json.dumps(weather_json)  # Return updated JSON as string
    except (json.JSONDecodeError, TypeError):
        return row['weather_data']  # Return as-is if decoding fails
    
# Create a default figure (just a simple map with a central location)
default_fig_map = go.Figure(go.Scattermapbox(
    lat=[56.1496278],  # Latitude of Denmark (example)
    lon=[10.2134046],  # Longitude of Denmark (example)
    mode='markers',
    marker=dict(size=10, color='blue', opacity=0.7),
    text=['Initial Map View'],
    customdata=['Denmark'],
    hoverinfo='text'
))

# Set up map layout settings
default_fig_map.update_layout(
    mapbox=dict(style="carto-positron", center=dict(lat=56.1496278, lon=10.2134046), zoom=6),
    margin=dict(l=10, r=10, t=10, b=10),
    height=700
)

app = dash.Dash(__name__, suppress_callback_exceptions=True)

# Example Dropdown Options for Production Types
production_columns = [
    'offshore_wind_mwh_gte_100',
    'offshore_wind_mwh_lt_100',
    'onshore_wind_power_mwh',
    'solar_power_mwh',
    'thermal_power_production'
]

industry_group_options = [
    {'label': 'Privat', 'value': 'Privat'},
    {'label': 'Offentligt', 'value': 'Offentligt'},
    {'label': 'Erhverv', 'value': 'Erhverv'},
]

weather_data_options = [
    'temp',
    'dwpt',
    'rhum',
    'prcp',
    'snow',
    'wdir',
    'wspd',
    'wpgt',
    'pres',
    'tsun',
    'coco',
]

import datetime

#weather_df = weather_df.withColumn("weather_data_parsed", from_json(col("weather_data"))) \
#                           .withColumn("windspeed", col("weather_data_parsed.wspd")) \
#                           .withColumn("date", col("timestamp").cast(DateType()))

#def standardize_series(series): #As our series are on potentially different scales, 
    #they are standardized 
    #by their Z-scores.
#    return (series - np.mean(series)) / np.std(series)

import plotly.graph_objects as go

# Ensure we start with the correct data in 'weather_data' column
def clean_weather_data(row):
    """
    Cleans and safely parses the 'weather_data' JSON-like string from a DataFrame row.
    Handles errors and invalid entries gracefully.
    """
    try:
        # Ensure the value is a valid JSON string
        if isinstance(row, str):
            return json.loads(row)
    except (json.JSONDecodeError, TypeError):
        # Return None for invalid JSON
        return None
    return None

# Create an initial empty map figure (you can adjust the initial view as needed)
fig_map = go.Figure(go.Scattermapbox(
    lat=[],
    lon=[],
    mode='markers',
    marker={'size': 10, 'opacity': 0.7},
    text=[]  # This can be the hover text if necessary
))

# Set map layout (adjust this based on your requirements)
fig_map.update_layout(
    mapbox=dict(
        style="open-street-map",  # You can change the map style here
        center={"lat": 55.6761, "lon": 12.5683},  # Initial center of the map (adjust as necessary)
        zoom=6  # Set zoom level
    ),
    title="Initial Map View"
)

def is_valid_click(click_data):
    if not click_data:
        return False
    
    # Extract longitude and latitude
    lon = click_data['points'][0].get('lon', None)
    lat = click_data['points'][0].get('lat', None)
    
    if lon is None or lat is None:
        return False

    # Define valid locations by longitude and latitude ranges
    valid_locations = {
        "DK1": {"lon_range": (8, 10), "lat_range": (54, 56)},
        "DK2": {"lon_range": (13, 15), "lat_range": (54, 56)},
        "Great Britain": {"lon_range": (-2, 1), "lat_range": (50, 59)},
        "Germany": {"lon_range": (11, 14), "lat_range": (47, 55)},
        "Netherlands": {"lon_range": (3, 6), "lat_range": (50, 54)},
        "Norway": {"lon_range": (8, 10), "lat_range": (58, 62)}
    }

    # Check if the clicked coordinates fall into any valid location
    for location, bounds in valid_locations.items():
        if (bounds["lon_range"][0] <= lon <= bounds["lon_range"][1]) and \
           (bounds["lat_range"][0] <= lat <= bounds["lat_range"][1]):
            return True

    return False

def get_location_from_click(click_data):
    # Verify click_data has the expected structure
    if not click_data or 'points' not in click_data or not click_data['points']:
        return None  # Return None if click_data is invalid

    # Extract lon and lat
    lon = click_data['points'][0].get('lon')
    lat = click_data['points'][0].get('lat')

    if lon is None or lat is None:
        return None  # Return None if lon/lat are missing

    # Map (lon, lat) to locations
    if 7 < lon < 12 and 54 < lat < 57:
        return "DK1"
    elif 12 < lon < 15 and 54 < lat < 57:
        return "DK2"
    elif lon < 2 and lat > 50:  # Example for Great Britain
        return "Great Britain"
    elif 11 < lon < 14 and 47 < lat < 55:  # Example for Germany
        return "Germany"
    elif 3 < lon < 6 and 50 < lat < 53:  # Example for Netherlands
        return "Netherlands"
    elif 4 < lon < 10 and 58 < lat < 63:  # Example for Norway
        return "Norway"

    # If no matches
    return None

@app.callback(
    Output('municipality-map', 'figure'),
    [   
        Input('day-slider', 'value'),
        Input('municipality-dropdown-store', 'data'),
        Input('municipality-map', 'clickData'),
        Input('upper-right-dropdown', 'value'),  # "Production" or "Consumption"
    ],
    [
        State('production-type-store', 'data'),
        State('production-state', 'data'), 
        State('consumption-state', 'data'),  
        State('import-export-state', 'data'), 
        State('weather-data-state', 'data'),  
        State('industry-group-store', 'data'),
        State('weather-data-store', 'data')
    ],
    prevent_initial_call=True
)

def update_map(
    selected_day,
    selected_municipality=None,
    click_data=None,
    selected_value='production',  # Default to 'production'
    selected_production_type=None,  # Relevant for Production mode
    production_state=1,  # Default production state: 1 means 'Production' is active
    consumption_state=0,  # Default consumption state: 0 means 'Consumption' is inactive
    import_export_state=0,  # Default import/export state: 0 means 'Power Import/Export' is inactive
    weather_data_state=0,
    selected_industry_group=None,  # Only used in Consumption mode
    selected_weather_property=None #Used in Weather Data mode
):
    dots = []
    hover_texts = []

    active_mode = None
    
    if production_state == 1:
        active_mode = 'production'
    elif consumption_state == 1:
        active_mode = 'consumption'
    elif import_export_state == 1:
        active_mode = 'import_export'
    elif weather_data_state == 1:
        active_mode = 'weather_data'
    else:
        active_mode = selected_value

    
    clicked_location = None
    # Check if click_data exists and contains the necessary information
    if click_data:
        # Extract longitude and latitude from the click_data
        lon = click_data['points'][0].get('lon', None)
        lat = click_data['points'][0].get('lat', None)
        
        if lon is not None and lat is not None:
            # Use the latitude and longitude to determine the location
            clicked_location = get_location_from_click(click_data)
        
    #Logic to deal with municipality_map clicks
    if clicked_location in municipality_map:
        selected_municipality = clicked_location
        print(f"Selected Municipality: {selected_municipality}")
    else:
        selected_municipality = None
        print("No valid municipality selected.")
    
    production_sizes = []
    production_colors = []
    production_hover_texts = []
    consumption_sizes = []
    consumption_colors = []
    consumption_hover_texts = []
    import_export_sizes = []
    import_export_colors = []
    import_export_hover_texts = []
    weather_data_sizes = []
    weather_data_colors = []
    weather_data_hover_texts = []

    if active_mode == 'production':
        day_filtered_data = production_pd[production_pd['hour_utc'].dt.floor('d') == selected_date]
        for _, row in municipality_df.iterrows():
            municipality_code = row['lau-1 code 1']
            production_value = day_filtered_data.loc[
            day_filtered_data['municipality_number'] == municipality_code,
            selected_production_type].sum()

            production_value_kwh = production_value * 1000
            scaled_size = np.log(np.sqrt(production_value) + 1) * 7
            hover_text = f"{row['hover_text']}<br>Municipality Number: {municipality_code}<br>Production Type: {selected_production_type}<br>Power Production: {production_value_kwh:.2f} kWh<br>Selected Day: {selected_date:%Y-%m-%d}"
            production_hover_texts.append(hover_text)
            production_sizes.append(scaled_size)
            if selected_production_type == 'thermal_power_production':
                color = px.colors.sequential.Reds
            elif selected_production_type == 'solar_power_mwh':
                color = px.colors.sequential.YlOrBr
            elif selected_production_type == 'onshore_wind_power_mwh':
                color = px.colors.sequential.Teal
            elif selected_production_type == 'offshore_wind_mwh_gte_100' or 'offshore_wind_mwh_lt_100':
                color = px.colors.sequential.Blues
            color_scale = np.interp(scaled_size, (min(production_sizes), max(production_sizes)), (0, 1))
            production_colors.append(color[int(color_scale * (len(color) - 1))])

    elif active_mode == 'consumption':
        if selected_industry_group:
            day_filtered_data = consumption_pd[
                (consumption_pd['hour_utc'].dt.floor('d') == selected_date) & 
                (consumption_pd['industry_groups'] == selected_industry_group)
            ]
        else:
            day_filtered_data = consumption_pd[consumption_pd['hour_utc'].dt.floor('d') == selected_date]

        for _, row in municipality_df.iterrows():
            municipality_code = row['lau-1 code 1']
            consumption_value = day_filtered_data.loc[
                day_filtered_data['municipality_number'] == municipality_code,
                'consumption_kwh'
            ].sum()            

            scaled_size = np.log(np.sqrt(consumption_value) + 1) * 4
            hover_text = f"{row['hover_text']}<br>Municipality Number: {municipality_code}<br>Industry Group: {selected_industry_group}<br>Power Consumption: {consumption_value:.2f} kWh<br>Selected Day: {selected_date:%Y-%m-%d}"
            consumption_hover_texts.append(hover_text)
            consumption_sizes.append(scaled_size)
            if selected_industry_group == 'Privat':
                color = px.colors.sequential.Oranges
            elif selected_industry_group == 'Offentligt':
                color = px.colors.sequential.Agsunset
            elif selected_industry_group == 'Erhverv':
                color = px.colors.sequential.Reds
            color_scale = np.interp(scaled_size, (min(consumption_sizes), max(consumption_sizes)), (0.3, 1))
            consumption_colors.append(color[int(color_scale * (len(color) - 1))])

    elif active_mode == 'import_export':
        filtered_import_export_data = prodcon_pd[
        (prodcon_pd['price_area'].isin(['DK1', 'DK2'])) &
        (prodcon_pd['hour_utc'].dt.date == selected_date.date())
        ]

        clicked_location = None
        if is_valid_click(click_data):
            clicked_location = click_data['points'][0]['lat'], click_data['points'][0]['lon']
            print(click_data)
        
        print(click_data)
                    # Combine DK1/DK2 and other country dots into a single loop
        all_locs = [
    # DK1 and DK2
        {'name': 'DK1', 'lat': 56.1496, 'lon': 10.2134, 'is_dk': True},
        {'name': 'DK2', 'lat': 55.6761, 'lon': 12.5683, 'is_dk': True},
        {'name': 'Germany', 'lat': 52.52, 'lon': 13.405, 'export_column': 'exchangege_mwh', 'import_column': 'exchangenl_mwh', 'is_dk': False},
        {'name': 'Netherlands', 'lat': 52.3676, 'lon': 4.9041, 'export_column': 'exchangenl_mwh', 'import_column': 'exchangege_mwh', 'is_dk': False},
        {'name': 'Norway', 'lat': 60.472, 'lon': 8.4689, 'export_column': 'exchangeno_mwh', 'import_column': 'exchangege_mwh', 'is_dk': False},
        {'name': 'Great Britain', 'lat': 51.5074, 'lon': -0.1278, 'export_column': 'exchangegb_mwh', 'import_column': 'exchangege_mwh', 'is_dk': False}
        ]

        start_date = pd.Timestamp('2022-01-01')
        end_date = pd.Timestamp('2022-12-31')
        selected_date1 = pd.Timestamp('2022-01-01') + pd.Timedelta(days=selected_day - 1)

        for loc in all_locs:
            if loc['is_dk']:
            # For DK1/DK2
                hover_texts = []
                for country in neighboring_countries.keys():
                    export_value = filtered_import_export_data.loc[
                        (filtered_import_export_data['price_area'] == loc['name']) &
                        (filtered_import_export_data['hour_utc'].dt.date <= selected_date1.date()) &
                        (filtered_import_export_data[country] > 0),
                        country
                    ].sum()
                    import_value = filtered_import_export_data.loc[
                        (filtered_import_export_data['price_area'] == loc['name']) &
                        (filtered_import_export_data['hour_utc'].dt.date <= selected_date1.date()) &
                        (filtered_import_export_data[country] < 0),
                        country
                    ].sum()

                    import_export_hover_texts.append(f"Import from {neighboring_countries[country]['name']}: {import_value:.2f} MWh<br>"
                               f"Export to {neighboring_countries[country]['name']}: {export_value:.2f} MWh<br>")

                dots.append(go.Scattermapbox(
                    lat=[loc['lat']],
                    lon=[loc['lon']],
                    mode='markers',
                    marker=dict(size=20, color='blue', opacity=0.7),
                    text=f"{loc['name']}<br>Daily values for Selected Day:{selected_date1}<br>{''.join(hover_texts)}",
                    hoverinfo='text'
                ))
            else:
                # For neighboring countries
                if loc['name'] == 'Great Britain':
                    import_value = filtered_import_export_data[loc['export_column']].sum()
                    export_value = filtered_import_export_data[loc['import_column']].sum()
                    cumulative_import = prodcon_pd.loc[prodcon_pd['hour_utc'].dt.date <= selected_date1.date(), loc['export_column']].sum()
                    cumulative_export = prodcon_pd.loc[prodcon_pd['hour_utc'].dt.date <= selected_date1.date(), loc['import_column']].sum()
                    yearly_max_import = prodcon_pd.loc[prodcon_pd['hour_utc'].dt.date <= end_date.date(), loc['export_column']].sum()
                    yearly_max_export = prodcon_pd.loc[prodcon_pd['hour_utc'].dt.date <= end_date.date(), loc['import_column']].sum()
                else:
                    export_value = filtered_import_export_data[loc['export_column']].sum()
                    import_value = filtered_import_export_data[loc['import_column']].sum()
                    cumulative_import = prodcon_pd.loc[prodcon_pd['hour_utc'].dt.date <= selected_date1.date(), loc['import_column']].sum()
                    cumulative_export = prodcon_pd.loc[prodcon_pd['hour_utc'].dt.date <= selected_date1.date(), loc['export_column']].sum()
                    yearly_max_import = prodcon_pd.loc[prodcon_pd['hour_utc'].dt.date <= end_date.date(), loc['import_column']].sum()
                    yearly_max_export = prodcon_pd.loc[prodcon_pd['hour_utc'].dt.date <= end_date.date(), loc['export_column']].sum()
                
                yearly_max = abs(yearly_max_import) + abs(yearly_max_export)

                current_use = abs(cumulative_import) + abs(cumulative_export)
                
                marker_color = 'green' if current_use < 0.8 * yearly_max else 'yellow' if current_use < 0.95 * yearly_max else 'red'
                hover_text = (f"{loc['name']}<br>Latitude:{loc['lat']}<br>Longitude:{loc['lon']}<br>Cumulative Trade Numbers up until Selected Day: {selected_date1}<br>"
                            f"Total Export to {loc['name']}: {cumulative_export:.2f} MWh<br>"
                            f"Total Import from {loc['name']}: {cumulative_import:.2f} MWh<br>"
                            f"Current Use: {current_use:.2f} MWh<br>Yearly Max: {yearly_max:.2f} MWh")

                dots.append(go.Scattermapbox(
                    lat=[loc['lat']],
                    lon=[loc['lon']],
                    mode='markers',
                    marker=dict(size=np.log(abs(cumulative_export + cumulative_import) + 1) * 7, color=marker_color, opacity=0.7),
                    text=hover_text,
                    hoverinfo='text'
                ))
    
    elif active_mode == 'weather_data':
        weather_pd['weather_data'] = weather_pd.apply(update_municipality, axis=1)

        # Parse the weather_data into a dictionary
        weather_pd['parsed_weather_data'] = weather_pd['weather_data'].apply(
            lambda x: json.loads(x) if isinstance(x, str) else None
        )

        # Extract the municipality from the parsed weather data
        weather_pd['parsed_municipality'] = weather_pd['parsed_weather_data'].apply(
            lambda x: x.get('municipality') if isinstance(x, dict) else None
        )
        if selected_weather_property:
            for selected_municipality in valid_municipalities:
    # Filter by selected municipality and selected date (assume selected_date is defined)
            
                filtered_data = weather_pd[
                (weather_pd['parsed_municipality'] == selected_municipality) &  # Municipality filter
                (weather_pd['timestamp'].dt.date == selected_date.date())  # Date filter
                ]
    
                weather_property_value = filtered_data['parsed_weather_data'].apply(
                    lambda x: x.get(selected_weather_property) if isinstance(x, dict) else np.nan
                )

                weather_property_daily_sum = weather_property_value.sum()
    
                weather_mean = weather_property_value.mean()
                weather_std = weather_property_value.std()
    
                scaled_size = abs((weather_property_value - weather_mean) / weather_std)*15
    
                weather_data_sizes.extend(scaled_size)
                hover_text = f"<br>Municipality Center for Weather Data Collection: {selected_municipality}<br>Weather Property: {selected_weather_property}<br>Sum of {selected_weather_property} for the Selected Day: {weather_property_daily_sum:.2f}<br>Selected Day: {selected_date:%Y-%m-%d}"
                weather_data_hover_texts.append(hover_text)
                if selected_weather_property == 'temp':
                    color = px.colors.sequential.thermal
                elif selected_weather_property == 'wspd':
                    color = px.colors.sequential.Darkmint
                elif selected_weather_property == 'rhum':
                    color = px.colors.sequential.PuBuGn
                elif selected_weather_property == 'prcp':
                    color = px.colors.sequential.PuBu
                elif selected_weather_property == 'coco':
                    color = px.colors.sequential.Greys
                elif selected_weather_property == 'wdir':
                    color = px.colors.sequential.Turbo
                elif selected_weather_property == 'pres':
                    color = px.colors.sequential.Agsunset
                elif selected_weather_property == 'snow':
                    color = px.colors.sequential.ice
                elif selected_weather_property == 'tsun':
                    color = px.colors.sequential.solar
                elif selected_weather_property == 'wpgt':
                    color = px.colors.sequential.speed
                elif selected_weather_property == 'dwpt':
                    color = px.colors.sequential.deep
    # Prevent NaN values in color scale
            color_scale = np.interp(scaled_size, (min(weather_data_sizes), max(weather_data_sizes)), (0.3, 1))
    
    # Ensure color scale is within the valid range
            color_index = np.clip((color_scale * (len(color) - 1)).astype(int), 0, len(color) - 1)
    
    # Append the corresponding color for each data point to the weather_data_colors list
            weather_data_colors.extend([color[i] for i in color_index])   
        
    if production_state == 1:
        sizes = production_sizes
        colors = production_colors
        hover_texts = production_hover_texts
    elif consumption_state == 1:
        sizes = consumption_sizes
        colors = consumption_colors
        hover_texts = consumption_hover_texts
    elif import_export_state == 1:
        sizes = import_export_sizes
        colors = import_export_colors
        hover_texts = import_export_hover_texts
    elif weather_data_state == 1:
        sizes = weather_data_sizes
        colors = weather_data_colors
        hover_texts = weather_data_hover_texts
    else:
        sizes = []
        colors = []
        hover_texts = []

    fig_map = go.Figure(go.Scattermapbox(
        lat=municipality_df['latitude'],
        lon=municipality_df['longitude'],
        mode='markers',
        marker=dict(size=sizes, color=colors, opacity=0.7),
        text=hover_texts,
        customdata=municipality_df['municipality'],
        hoverinfo='text'
    ))

    for dot in dots:
        fig_map.add_trace(dot)

    fig_map.update_layout(mapbox=dict(style="carto-positron", center=dict(lat=56.1496278, lon=10.2134046), zoom=6),
              margin=dict(l=10, r=10, t=10, b=10), height=550)

    return fig_map

import datetime
date_labels = [datetime.date(2024, 1, 1) + datetime.timedelta(days=i) for i in range(365)]
date_marks = {i+1: date_labels[i].strftime('%B %d') for i in range(0, 365, 30)}  # Show every 30th day

app.layout = html.Div([
    dcc.Store(id='production-type-store', data='offshore_wind_mwh_gte_100'),  # Default production type (can be updated by dropdown)
    dcc.Store(id='industry-group-store', data='Privat'),  # Store for selected industry group
    dcc.Store(id='weather-data-store', data='temp'),  # Store for selected weather data
    dcc.Store(id='municipality-dropdown-store', data=None),
    html.Div([
        dcc.Store(id='production-state', data=1),  # Default: 1 means 'Production' is active
        dcc.Store(id='consumption-state', data=0),  # Default: 0 means 'Consumption' is inactive
        dcc.Store(id='import-export-state', data=0),  # Set the initial data value to 0 (inactive state)
        dcc.Store(id='weather-data-state', data=0)  # Set the initial data value to 0 (inactive state)
    ]),
    html.Div([
        # Map and Day Slider on the left
        html.Div([
            dcc.Graph(
                id='municipality-map',
                figure=default_fig_map,  # Use the default figure initially
                style={'height': '80vh', 'width': '100%', 'padding': '0', 'margin': '0'}
            ),
            html.Div([
                html.H4("Select Day:", style={'marginBottom': '10px'}),
                dcc.Slider(
                    id='day-slider',
                    min=1,
                    max=365,
                    step=1,
                    value=1,  # Default value (January 1)
                    marks=date_marks,  # Slider marks showing the formatted date labels
                ),
            ], style={'padding': '10px', 'marginTop': '10px'})
        ], style={'width': '50%', 'display': 'inline-block', 'vertical-align': 'top'}),

        # Right-side content (Dropdowns and Graph)
        html.Div([
            html.Div([
                html.H4("Select a Property for the Municipality"),
                dcc.Dropdown(
                    id='upper-right-dropdown',
                    options=[
                        {'label': 'Production', 'value': 'production'},
                        {'label': 'Consumption', 'value': 'consumption'},
                        {'label': 'Power Import/Export', 'value': 'power import/export'},
                        {'label': 'Weather Data', 'value': 'weather data'},
                    ],
                    value='production',  # Default value is "production"
                ),
            ], style={'padding': '20px', 'marginBottom': '20px'}),
            html.Div(id='dynamic-content-container', style={
                'height': '80vh',
                'border': '1px solid black',
                'backgroundColor': '#f4f4f4',
                'padding': '20px',
                'textAlign': 'center',
                'color': '#888',
                'flex-grow': 1,
            }),
            html.Div([
                html.Div(
                    dcc.Dropdown(
                        id='industry-group-dropdown',
                        options=industry_group_options,
                        value='Privat',  # Default to 'Privat'
                    ),
                    id='industry-group-dropdown-container',
                    style={'display': 'none'}),
            ]),
            html.Div([
                html.Div(
                    dcc.Dropdown(
                        id='weather-data-dropdown',
                        options=[{'label': col, 'value': col} for col in weather_data_options],
                        value=weather_data_options[0],  # Default to the first production type
                    ),
                    id='weather-data-dropdown-container',
                    style={'display': 'none'}),
            ]),
            html.Div([
                html.Div(id='import-export-pie-charts', style={'margin-top': '20px', 'display': 'none'}),
                dcc.Graph(id='import-export-pie-chart', style={'margin-top': '20px', 'display': 'none'}),
            ], style={'display': 'none'}),  # Initially hidden; visibility will be toggled dynamically
            # Always include the production-type-dropdown in the layout
            html.Div([
                # This div is always present, but its visibility is toggled by the 'style' property
                html.Div(
                    dcc.Dropdown(
                        id='production-type-dropdown',
                        options=[{'label': col, 'value': col} for col in production_columns],
                        value=production_columns[0],  # Default to the first production type
                    ),
                    id='production-type-dropdown-container',
                    style={'display': 'none'}  # Initially hidden; visibility will be toggled dynamically
                ),
            ])
        ], style={'width': '50%', 'display': 'inline-block', 'vertical-align': 'top', 'padding': '20px'}),
    ], style={'display': 'flex', 'height': '100vh', 'backgroundColor': '#003366'}),
])

@app.callback(
    Output('production-type-store', 'data'),
    Input('production-type-dropdown', 'value')
)

def update_production_type_store(selected_production_type):
    # Store the selected production type in the Store component
    return selected_production_type

@app.callback(
    Output('consumption-state', 'data'),
    Output('import-export-state', 'data'),
    Output('production-state', 'data'),
    Output('weather-data-state', 'data'),
    [Input('production-state', 'data'),
     Input('consumption-state', 'data'),
     Input('import-export-state', 'data'),
     Input('weather-data-state', 'data'),
     Input('upper-right-dropdown', 'value')],
    prevent_initial_call=True
)

def toggle_upper_right_dropdown(production_state, consumption_state, import_export_state, weather_data_state, selected_value):
    if selected_value == 'production':
        return 0, 0, 1, 0
    elif selected_value == 'consumption':
        return 1, 0, 0, 0
    elif selected_value == 'power import/export':
        return 0, 1, 0, 0
    elif selected_value == 'weather data':
        return 0, 0, 0, 1
    
@app.callback(
    Output('production-type-dropdown-container', 'style'),
    [Input('production-state', 'data'),
     Input('consumption-state', 'data'),
     Input('import-export-state', 'data'),],
    prevent_initial_call=True
)

def toggle_production_type_dropdown(production_state, consumption_state, import_export_state):
    # Show the production-type-dropdown only when production is active
    if production_state == 1:
        return {'display': 'block'}  # Show dropdown
    else:
        return {'display': 'none'}   # Hide dropdown when consumption is inactive

@app.callback(
    Output('weather-data-dropdown-container', 'style'),
    [Input('production-state', 'data'),
     Input('consumption-state', 'data'),
     Input('import-export-state', 'data'),
     Input('weather-data-state', 'data')],
    prevent_initial_call=True
)

def toggle_weather_data_dropdown(production_state, consumption_state, import_export_state, weather_data_state):
    # Show the production-type-dropdown only when production is active
    if weather_data_state == 1:
        return {'display': 'block'}  # Show dropdown
    else:
        return {'display': 'none'}   # Hide dropdown when consumption is inactive

@app.callback(
    Output('industry-group-dropdown-container', 'style'),
    [Input('production-state', 'data'),
     Input('consumption-state', 'data'),
     Input('import-export-state', 'data'),],
    prevent_initial_call=True
)

def toggle_industry_group_dropdown(production_state, consumption_state, import_export_state):
    # Show the industry-group-dropdown only when production is active
    if consumption_state == 1:
        return {'display': 'block'}  # Show dropdown
    else:
        return {'display': 'none'}   # Hide dropdown when consumption is inactive

@app.callback(
    Output('industry-group-store', 'data'),  # Store the selected industry group
    Input('industry-group-dropdown', 'value'),
)

def update_industry_group_store(selected_industry_group):
    return selected_industry_group

@app.callback(
    Output('weather-data-store', 'data'),  # Store the selected industry group
    Input('weather-data-dropdown', 'value'),
)

def update_weather_data_store(selected_industry_group):
    return selected_industry_group

@app.callback(
    Output('dynamic-content-container', 'children'),
    Output('import-export-pie-charts', 'children'),  # Add output for pie charts
    Output('import-export-pie-charts', 'style'),  # Add output for pie charts style
    [Input('upper-right-dropdown', 'value'),
     Input('municipality-map', 'clickData'),
     Input('day-slider', 'value')]
)
def update_dynamic_content(selected_value, click_data, selected_day):
    pie_charts_style = {'margin-top': '20px', 'display': 'none'}
    pie_charts = []  # Initialize empty list for pie charts
    # Default message for dynamic content
    children = html.Div("Future plots and controls will appear here.")
    
    # Handle content for the "upper-right-dropdown"
    if selected_value == 'production':
        children = html.Div([
            html.Div([
                html.Label("Select Municipality:"),
                dcc.Dropdown(
                    id='municipality-dropdown',
                    options=[{'label': m, 'value': m} for m in municipality_map.values()],
                    value='København',  # Default to Copenhagen
                )
            ], style={'marginBottom': '20px'}),
            html.Div([
                html.Label("Select Production Type:"),
                dcc.Dropdown(
                    id='production-type-dropdown',
                    options=[{'label': col, 'value': col} for col in production_columns],
                    value=production_columns[0],  # Default to the first production type
                ),
            ], style={'marginBottom': '20px'}),
            dcc.Graph(id='production-yearly-plot')  # Placeholder for the yearly plot
        ])
            
    elif selected_value == 'consumption':
        children = html.Div([
            html.Div([
                html.Label("Select Municipality:"),
                dcc.Dropdown(
                    id='municipality-dropdown',
                    options=[{'label': m, 'value': m} for m in municipality_map.values()],
                    value='København',  # Default to Copenhagen
                )
            ], style={'marginBottom': '20px'}),
            html.Div([
                html.Label("Select Industry Group:"),
                dcc.Dropdown(
                    id='industry-group-dropdown',
                    options=[
                        {'label': 'Privat', 'value': 'Privat'},
                        {'label': 'Offentligt', 'value': 'Offentligt'},
                        {'label': 'Erhverv', 'value': 'Erhverv'},
                    ],
                    value='Privat',  # Default to "Privat"
                )
            ], style={'marginBottom': '20px'}),
            dcc.Graph(id='consumption-yearly-plot')  # Placeholder for the yearly consumption plot
        ])
            
    elif selected_value == 'power import/export':
        location = get_location_from_click(click_data)  # Extract location
        if not location:
            children = html.Div("Invalid location selected. Please click on DK1, DK2, or neighboring countries.")
            pie_charts_style = {'margin-top': '20px', 'display': 'none'}
            pie_charts = []  # No pie charts to display
        else:
            pie_charts = update_import_export_pie(location, selected_day)
            pie_charts_style = {'margin-top': '20px', 'display': 'block'}
            children = html.Div([*pie_charts])
    
    elif selected_value == 'weather data':
        children = html.Div([
            html.Div([
                html.Label("Select Municipality:"),
                dcc.Dropdown(
                    id='municipality-dropdown',
                    options=[{'label': m, 'value': m} for m in municipality_map.values()],
                    value='København',  # Default to Copenhagen
                )
            ], style={'marginBottom': '20px'}),
            html.Div([
                html.Label("Select Weather Property:"),
                dcc.Dropdown(
                    id='weather-data-dropdown',
                    options=[{'label': col, 'value': col} for col in weather_data_options],
                    value=weather_data_options[0],  # Default to the first production type
                ),
            ], style={'marginBottom': '20px'}),
            dcc.Graph(id='weather-yearly-plot')  # Placeholder for the yearly weather plot
        ])        
    
    return children, pie_charts, pie_charts_style       
     
@app.callback(
    Output('municipality-dropdown-store', 'data'),  # Update the store
    Input('municipality-map', 'clickData')          # Triggered by map click
)
def update_municipality_store(click_data):
    if click_data and 'customdata' in click_data['points'][0]:
        clicked_municipality = click_data['points'][0]['customdata']
        return clicked_municipality  # Update the store with the clicked municipality
    return dash.no_update  # Do not update if no click data is available

@app.callback(
    Output('municipality-dropdown', 'value'),  # Update the dropdown value
    Input('municipality-dropdown-store', 'data')  # Triggered by store updates
)
def sync_dropdown_with_store(store_value):
    return store_value or 'København'  # Default to 'København' if store value is None

# Callback to generate the yearly production plot
@app.callback(
    Output('production-yearly-plot', 'figure'),
    [Input('production-type-dropdown', 'value'),
     Input('municipality-dropdown', 'value'),
     Input('day-slider', 'value')]
)

def update_yearly_plot(selected_production_type, selected_municipality, selected_day):
    
    # Reverse the municipality_map for fast lookup
    municipality_map_reversed = {v: k for k, v in municipality_map.items()}
    
    # Get the municipality number
    selected_municipality_number = municipality_map_reversed.get(selected_municipality)
    selected_municipality_number = int(selected_municipality_number)
    if not selected_municipality_number:
        raise ValueError(
            f"Municipality '{selected_municipality}' not found. Please select a valid municipality."
        )

    filtered_data = production_pd[production_pd['municipality_number'] == selected_municipality_number]
    
    filtered_data['hour_utc'] = pd.to_datetime(filtered_data['hour_utc'])

    filtered_data = filtered_data.sort_values(by='hour_utc')

    selected_date = pd.Timestamp('2022-01-01') + pd.Timedelta(days=selected_day - 1)
    selected_hour_str = selected_date.isoformat()  # This is the selected date in ISO string format

    print(f"Selected date: {selected_hour_str}")
    
    if selected_hour_str not in filtered_data['hour_utc'].dt.strftime('%Y-%m-%dT%H:%M:%S').values:
        print(f"Error: Selected date {selected_hour_str} is not available in the filtered data.")
        print("Available dates in filtered data:")
        
    selected_index = filtered_data[filtered_data['hour_utc'].dt.strftime('%Y-%m-%dT%H:%M:%S') == selected_hour_str].index[0]
    
    if selected_production_type not in filtered_data.columns:
        print(f"Error: {selected_production_type} not found in filtered data columns.")
        return go.Figure()  # Return an empty plot if the production type is invalid

    time_series = filtered_data['hour_utc']
    production_series = filtered_data[selected_production_type]
    production_series = production_series.fillna(0)
        
    from scipy.fftpack import dct, idct
    try:
        production_series = production_series.values
        dct_coefficients = dct(production_series, norm='ortho')
        dct_filtered = dct_coefficients.copy()
        dct_filtered[int(len(dct_coefficients) * 0.02):] = 0  # Keep the first 2% of coefficients
        smoothed_series = idct(dct_filtered, norm='ortho')
    except Exception as e:
        print(f"Error applying DCT: {e}")
        return go.Figure()  # Return an empty plot if DCT fails

    fig = go.Figure()

    fig.add_scatter(
        x=time_series,
        y=production_series,
        mode='lines',
        line=dict(color='gray', width=1),
        name='Raw Data',
        fill='tozeroy',
        fillcolor='rgba(166, 166, 166, 0.8)',
    )

    fig.add_scatter(
        x=time_series,
        y=smoothed_series,
        mode='lines',
        line=dict(color='green', width=4),
        name='Smoothed Data',
    )

    fig.add_vline(
        x=selected_hour_str,  # The ISO datetime string for the selected hour
        line=dict(color='red', width=2)
    )

    fig.update_layout(
        title=f"Power Production for {selected_municipality}",
        xaxis_title="Time",
        yaxis_title="Production (MWh)",
        xaxis=dict(showgrid=True),
        yaxis=dict(showgrid=True),
        legend_title="Legend",
    )

    return fig

# Callback to generate the yearly weather data plot
@app.callback(
    Output('weather-yearly-plot', 'figure'),
    [Input('weather-data-dropdown', 'value'),
     Input('municipality-dropdown', 'value'),
     Input('day-slider', 'value')]
)

def update_weather_yearly_plot(selected_weather_property, selected_municipality, selected_day):
    # Validate the selected municipality
    if not selected_municipality:
        raise ValueError(
            f"Municipality '{selected_municipality}' not found. Please select a valid municipality."
        )

    # Debugging information
    print(weather_pd['municipality'].unique())
    print(f"Selected municipality: {selected_municipality}")

    # Filter the data for the selected municipality
    filtered_data = weather_pd[weather_pd['municipality'] == selected_municipality]
    
    if filtered_data.empty:
        print(f"No data available for municipality: {selected_municipality}")
        return go.Figure()  # Return an empty plot if no data is available

    filtered_data = filtered_data.sort_values(by='timestamp')

    # Determine the selected date and hour
    selected_date = pd.Timestamp('2022-01-01') + pd.Timedelta(days=selected_day - 1)
    selected_date_datetime = selected_date.to_pydatetime()
    selected_hour_str = selected_date.isoformat()  # This is the selected date in ISO string format

    print(filtered_data['timestamp'].head())
    print(f"Selected date: {selected_hour_str}")

    # Find the index of the selected timestamp
    try:
        selected_index = filtered_data[filtered_data['timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%S') == selected_date_datetime].index[0]
    except IndexError:
        print(f"No data available for selected date: {selected_date_datetime}")
        return go.Figure()  # Return an empty plot if no data is available for the selected date

    # Validate the selected weather property
    if selected_weather_property not in filtered_data['weather_data'].iloc[0]:
        print(f"Error: {selected_weather_property} not found in weather data.")
        return go.Figure()  # Return an empty plot if the property is invalid

    # Prepare time series and weather series
    time_series = filtered_data['timestamp']
    weather_series = filtered_data['weather_data'].apply(lambda x: x.get(selected_weather_property, 0))

    # Perform Discrete Cosine Transform (DCT) smoothing
    try:
        weather_series = weather_series.values
        dct_coefficients = dct(weather_series, norm='ortho')
        dct_filtered = dct_coefficients.copy()
        dct_filtered[int(len(dct_coefficients) * 0.02):] = 0  # Keep the first 2% of coefficients
        smoothed_series = idct(dct_filtered, norm='ortho')
    except Exception as e:
        print(f"Error applying DCT: {e}")
        return go.Figure()  # Return an empty plot if DCT fails

    # Create the plot
    fig = go.Figure()

    # Add raw data to the plot
    fig.add_scatter(
        x=time_series,
        y=weather_series,
        mode='lines',
        line=dict(color='gray', width=1),
        name='Raw Data',
        fill='tozeroy',
        fillcolor='rgba(166, 166, 166, 0.8)',
    )

    # Add smoothed data to the plot
    fig.add_scatter(
        x=time_series,
        y=smoothed_series,
        mode='lines',
        line=dict(color='green', width=4),
        name='Smoothed Data',
    )

    # Add a vertical line for the selected date
    fig.add_vline(
        x=selected_date_datetime,
        line=dict(color='red', width=2),
        annotation_text="Selected Hour",
        annotation_position="top left",
    )

    # Customize layout
    fig.update_layout(
        title=f"Weather Property Development for {selected_municipality}",
        xaxis_title="Time",
        yaxis_title="Value",
        xaxis=dict(showgrid=True),
        yaxis=dict(showgrid=True),
        legend_title="Legend",
    )

    return fig
    
@app.callback(
    Output('consumption-yearly-plot', 'figure'),
    [
        Input('municipality-dropdown', 'value'),
        Input('industry-group-dropdown', 'value'),  # Added Input for industry group
        Input('day-slider', 'value')
    ]
)
def update_consumption_yearly_plot(selected_municipality, selected_industry_group, selected_day):
    # Reverse the municipality_map for fast lookup
    municipality_map_reversed = {v: k for k, v in municipality_map.items()}
    selected_municipality_number = municipality_map_reversed.get(selected_municipality)

    if not selected_municipality_number:
        return go.Figure()  # Return an empty plot if municipality not found

    # Filter data for selected municipality and industry group
    filtered_data = consumption_pd[
        (consumption_pd['municipality_number'] == int(selected_municipality_number)) &
        (consumption_pd['industry_groups'] == selected_industry_group)
    ]

    filtered_data['hour_utc'] = pd.to_datetime(filtered_data['hour_utc'])
    filtered_data = filtered_data.sort_values(by='hour_utc')

    # Get the selected date
    selected_date = pd.Timestamp('2022-01-01') + pd.Timedelta(days=selected_day - 1)
    selected_hour_str = selected_date.isoformat()

    # Prepare time series
    time_series = filtered_data['hour_utc']
    consumption_series = filtered_data['consumption_kwh'].fillna(0)

    # Smooth the data using DCT
    try:
        consumption_values = consumption_series.values
        dct_coefficients = dct(consumption_values, norm='ortho')
        dct_filtered = dct_coefficients.copy()
        dct_filtered[int(len(dct_coefficients) * 0.02):] = 0  # Keep the first 2% of coefficients
        smoothed_series = idct(dct_filtered, norm='ortho')
    except Exception as e:
        print(f"Error applying DCT: {e}")
        return go.Figure()  # Return an empty plot if DCT fails

    # Create the figure
    fig = go.Figure()

    # Add raw data trace
    fig.add_scatter(
        x=time_series,
        y=consumption_series,
        mode='lines',
        line=dict(color='gray', width=1),
        name='Raw Data',
        fill='tozeroy',
        fillcolor='rgba(166, 166, 166, 0.8)',
    )

    # Add smoothed data trace
    fig.add_scatter(
        x=time_series,
        y=smoothed_series,
        mode='lines',
        line=dict(color='red', width=4),
        name='Smoothed Data',
    )

    # Add vertical line for the selected day
    fig.add_vline(
        x=selected_hour_str,
        line=dict(color='red', width=2),
        name='Selected Day'
    )

    # Update layout
    fig.update_layout(
        title=f"Power Consumption for {selected_municipality} ({selected_industry_group})",
        xaxis_title="Time",
        yaxis_title="Consumption (MWh)",
        xaxis=dict(showgrid=True),
        yaxis=dict(showgrid=True),
        legend_title="Legend",
    )

    return fig

def update_import_export_pie(selected_location, selected_day):
    selected_price_area = "DK1" if selected_location == "DK1" else "DK2"

    # Filter data
    selected_date = pd.Timestamp('2022-01-01') + pd.Timedelta(days=selected_day - 1)
    filtered_import_export_data = prodcon_pd[
        (prodcon_pd['price_area'] == selected_price_area) & 
        (prodcon_pd['hour_utc'].dt.date == selected_date.date())
    ]
    
    # Aggregate exchange values
    exchange_columns = ['exchangegb_mwh', 'exchangege_mwh', 'exchangenl_mwh', 'exchangeno_mwh']
    exchanges = filtered_import_export_data[exchange_columns].sum()
    selected_date1 = pd.Timestamp('2022-01-01') + pd.Timedelta(days=selected_day - 1)
    
    # Prepare pie chart data
    pie_charts = []
    country_labels = {
        'exchangegb_mwh': 'Great Britain',
        'exchangege_mwh': 'Germany',
        'exchangenl_mwh': 'Netherlands',
        'exchangeno_mwh': 'Norway'
    }

    if selected_location in ["DK1", "DK2"]:
        for col in exchange_columns:
            import_value = filtered_import_export_data[col][filtered_import_export_data[col] < 0].sum()
            export_value = filtered_import_export_data[col][filtered_import_export_data[col] > 0].sum()
            pie_charts.append(
                html.Div(
                    dcc.Graph(
                        figure=go.Figure(
                            data=[
                                go.Pie(
                                    labels=['Import', 'Export'],
                                    values=[abs(import_value), abs(export_value)],
                                    hole=0.3,
                                    marker=dict(colors=['red', 'green']),
                                    textinfo='label+percent',
                                    insidetextorientation='radial',
                                    textposition='inside',
                                    textfont=dict(size=14)
                                )
                            ],
                            layout=go.Layout(
                                title=f"{country_labels[col]} Import/Export",
                                margin=dict(l=15, r=15, t=40, b=10),
                                height=300,
                                showlegend=True
                            )
                        ),
                        style={'height': '30vh'}
                    ),
                    style={'display': 'inline-block', 'width': '45%'}
                )
            )
    else:
        for col, country in neighboring_countries.items():
            if country['name'] == selected_location:
                filtered_import_export_data2 = prodcon_pd[
                    (prodcon_pd['hour_utc'].dt.date == selected_date.date())
                ]    
                import_value = filtered_import_export_data2[col][filtered_import_export_data2[col] < 0].sum()
                export_value = filtered_import_export_data2[col][filtered_import_export_data2[col] > 0].sum()
                cumulative_import = prodcon_pd.loc[prodcon_pd['hour_utc'].dt.date <= selected_date1.date(), col][prodcon_pd[col] < 0].sum()
                cumulative_export = prodcon_pd.loc[prodcon_pd['hour_utc'].dt.date <= selected_date1.date(), col][prodcon_pd[col] > 0].sum()
                pie_charts.append(
                    html.Div(
                        dcc.Graph(
                            figure=go.Figure(
                                data=[
                                    go.Pie(
                                        labels=['(MWh) Import from', '(MWh) Export to'],
                                        values=[abs(cumulative_import), abs(cumulative_export)],
                                        hole=0.3,
                                        marker=dict(colors=['red', 'green']),
                                        textinfo='label+percent',
                                        insidetextorientation='radial',
                                        textposition='inside',
                                        textfont=dict(size=14)
                                    )
                                ],
                                layout=go.Layout(
                                    title=f"Denmark Import/Export from and to {country['name']}",
                                    margin=dict(l=15, r=15, t=40, b=10),
                                    height=230,
                                    showlegend=True
                                )
                            ),
                            style={'height': '60vh'}
                        ),
                        style={'display': 'inline-block', 'width': '100%'}
                    )
                )
                break
    return pie_charts

if __name__ == '__main__':
    app.run_server(debug=True)