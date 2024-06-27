from flask import Flask,request, render_template, redirect, url_for, session
import requests
import os
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler
import logging
from db import Database
from google.cloud import bigquery, storage
from google.cloud.exceptions import NotFound
from flatten_json import flatten
import pandas as pd
import json

# Service account with correct permissions
GOOGLE_APPLICATION_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
# Start flask application
app = Flask(__name__)
# Load env variables
load_dotenv()
api_key = os.getenv('API_KEY')
app.secret_key = os.getenv('SECRET_KEY')
project_id = 'festive-idea-426808-e6'
dataset_id = 'VMO2_Dataset'
table_id = 'weather_table'
bucket_name = 'vmo2_bucket'
source_file_name = 'output.csv'
destination_blob_name = 'output.csv'
source_uri = f'gs://{bucket_name}/{destination_blob_name}'
# project_id = 'festive-idea-426808-e6'
# dataset_id = f'{project_id}.VMO2_Dataset'
# table_id = f'{dataset_id}.weather_table'
# bucket_name = 'vmo2_bucket'
# source_file_name = 'output.csv'
# destination_blob_name = 'output.csv'
# source_uri = f'gs://{bucket_name}/{destination_blob_name}'

# Set up logging
if not app.debug:
    handler = RotatingFileHandler('error.log', maxBytes=10000, backupCount=1)
    handler.setLevel(logging.ERROR)
    app.logger.addHandler(handler)

# BigQuery and Storage Clients 
client = bigquery.Client()
storage_client = storage.Client()

# Check if bucket exists. If not, call create_bucket
def check_if_bucket_exists():
    try:
        bucket = storage_client.get_bucket(bucket_name)
        if bucket:
            print(f'Bucket {bucket_name} exists')
    except NotFound as e:
        print(f'Bucket {bucket_name} does not exist')
        print(f'Creating bucket {bucket_name}...')
        create_bucket()

# Create bucket
def create_bucket():
    try:
        bucket = storage_client.create_bucket(bucket_name)
        print(f'Bucket {bucket.name} created')
    except Exception as e:
        print(f'Error creating bucket: {e}')

#                 Bucket name. Path to csv file. Name of the file to be uploaded
def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_filename(source_file_name)
        print(f'File {source_file_name} uploaded to {destination_blob_name}')
    except Exception as e:
        print(f'Error uploading file to GCS: {e}')

# BigQuery table schema will be auto generated based off the csv file
def load_csv_from_storage_to_bigquery(dataset_id, table_id, source_uri):
    table_ref = client.dataset(dataset_id).table(table_id)
    job_config = bigquery.LoadJobConfig(
        source_format = bigquery.SourceFormat.CSV,
        autodetect = True,
    )

    load_job = client.load_table_from_uri(
        source_uri, table_ref, job_config=job_config
    )

    print(f'Staring job {load_job.job_id}')
    load_job.result()

    print('Job Finished')
    table = client.get_table(table_ref)
    print(f"Loaded {table.num_rows} rows to {dataset_id}:{table_id}")


# Convert weather_data which is json into a csv file
def create_csv_file(weather_data):
    # Flatten weather data
    flat_json = flatten(weather_data)
    # Pandas DataFrame
    df = pd.DataFrame(flat_json, index=[0])
    csv_file = 'output.csv'
    df.to_csv(csv_file, index=True)
    

def check_if_dataset_exists():
    try:
        dataset = client.get_dataset(dataset_id)
        if dataset:
            print(f'Dataset {dataset_id} exists')
    except NotFound as e:
        print(f'Dataset {dataset_id} does not exist')
        print(f'Creating dataset {dataset_id}...')
        create_dataset()
        
    
def create_dataset():
    try:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        dataset = client.create_dataset(dataset, timeout=30)
        print(f'Dataset {dataset.dataset_id} created')
    except Exception as e:
        print(f'Error creating dataset: {e}')
    
# def check_if_table_exists():
#     try:
#         table = client.get_table(table_id)
#         if table:
#             print(f'Table {table_id} exists')
#     except NotFound as e:
#         print(f'Table {table_id} does not exist')
#         print(f'Creating table {table_id}...')
#         create_table()


# Loads the main page
@app.route('/', methods=['GET'])
def index():
    check_if_bucket_exists()
    check_if_dataset_exists()
    # check_if_table_exists()
    return render_template('index.html')

# Loads error page with custom message
@app.route('/error', methods=['GET'])
def error():
    message = request.args.get('message', 'An error occured')
    return render_template('error.html', message=message)

# Gets the users location from the index.html page
@app.post('/location')
def location():
    try:
        location_name = request.form['location']
        if not location_name:
            app.logger.error(f'Error fetching location')
            return redirect(url_for('error', message='Error parsing geolocation data.'))
        else:
            return redirect(url_for('geo_data', location_name=location_name))
    except Exception as e:
        app.logger.exception(f"Exception occured while getting location from user: {e}")
        return redirect(url_for('error', message='Error parsing geolocation data.'))


# Calls the geocoding api and retuns the latitude and longitude
@app.route('/geo_data/<location_name>')
def geo_data(location_name):
    try:
        geo_url = f'https://api.openweathermap.org/geo/1.0/direct?q={location_name}&limit=1&appid={api_key}'
        data = requests.get(geo_url)
        location_json = data.json()
        lat = location_json[0].get('lat')
        lon = location_json[0].get('lon')
        return redirect(url_for('get_weather_response', lat = lat, lon = lon))
    except requests.RequestException as e:
        app.logger.exception(f'Exception thrown when making API request to Geocoding API:{e}')
        return redirect(url_for('error', message=f'Exception thrown when making API request to Geocoding API: {e}'))
    except Exception as e:
        app.logger.exception(f'Unkown exception thrown when making API request to Geocoding API:{e}')
        return redirect(url_for('error', message=f'Unknwon exception thrown when making API request to Geocoding API: {e}'))
    
# Gets the weather data
@app.route('/get_weather')
def get_weather_response():
    try:
        # Make API call to Weather API
        lat = request.args.get('lat')
        lon = request.args.get('lon')
        url = f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}'
        data = requests.get(url)
        weather_data = data.json()
        
        create_csv_file(weather_data)
        print('create_csv_file has been ran')
        upload_to_gcs(bucket_name, source_file_name, destination_blob_name)
        print('upload_to_gcs has been ran')
        load_csv_from_storage_to_bigquery(dataset_id, table_id, source_uri)
        print('load_csv_from_storage_to_bigquery has been ran')

        # Track data across multiple requests
        session['weather_data'] = weather_data
        return redirect(url_for('get_weather_for_user'))
    except requests.RequestException as e:
        app.logger.exception(f'Exception thrown when making API requset to Weather API: {e}')
        return redirect(url_for('error', message=f'Exception thrown when making API requset to Weather API: {e}'))
    except Exception as e:
        app.logger.exception(f'Unkown exception thrown when making API requset to Weather API:{e}')
        return redirect(url_for('error', message=f'Unknwon exception thrown when making API requset to Weather API: {e}'))

@app.route('/get_weather_for_user')
def get_weather_for_user():
    try:
        # Retrieve data from session
        weather_data = session.get('weather_data')
        print(weather_data)
        clouds = weather_data.get('clouds', {}).get('all', 'N/A')
        forecast = weather_data.get('weather', {})[0].get('description', 'N/A')
        wind_speed = weather_data.get('wind').get('speed', 'N/A')
        location = weather_data.get('name', 'N/A')
        temp = weather_data.get('main', {}).get('feels_like', 'N/A')
        temp = (temp - 273.15)
        temp = f'{temp:.2f}'         
        return render_template('weather.html', clouds=clouds, temp=temp, forecast=forecast, wind_speed=wind_speed, location=location)
    except KeyError as e:
        app.logger.exception(f'KeyError: {e}')
        return redirect(url_for('error', message=f'KeyError: {e}'))
    except TypeError as e:
        app.logger.exception(f'TypeError: {e}')
        return redirect(url_for('error', message=f'TypeError: {e}'))
    except ValueError as e:
        app.logger.exception(f'ValueError: {e}')
        return redirect(url_for('error', message=f'ValueError: {e}'))
    except Exception as e:
        app.logger.exception(f'Unkown exception: {e}')
        return redirect(url_for('error', message=f'Unkown exception: {e}'))


if __name__ == "__main__":
    app.run(debug=True)
