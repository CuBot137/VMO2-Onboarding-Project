from flask import Flask,request, render_template, redirect, url_for, session
import requests
import os
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler
import logging
from db import Database
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

GOOGLE_APPLICATION_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
# Start flask application
app = Flask(__name__)
# Load env variables
load_dotenv()
api_key = os.getenv('API_KEY')
app.secret_key = os.getenv('SECRET_KEY')
project_id = 'festive-idea-426808-e6'
dataset_id = f'{project_id}.VMO2_Dataset'
table_id = f'{dataset_id}.weather_table'

# Set up logging
if not app.debug:
    handler = RotatingFileHandler('error.log', maxBytes=10000, backupCount=1)
    handler.setLevel(logging.ERROR)
    app.logger.addHandler(handler)

client = bigquery.Client()

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
    
def check_if_table_exists():
    try:
        table = client.get_table(table_id)
        if table:
            print(f'Table {table_id} exists')
    except NotFound as e:
        print(f'Table {table_id} does not exist')
        print(f'Creating table {table_id}...')
        create_table()

def create_table():
    try:
        schema = [
            bigquery.SchemaField("id", "INTEGER", mode="REQUIRED", policy_tags=["AUTOMATIC"]),
            bigquery.SchemaField("data", "STRING", mode="REQUIRED")
        ]
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)
        print(f'Table {table.table_id} created')
    except Exception as e:
        print(f'Error creating table: {e}')

def send_data_to_bigquery(data):
    try:
        rows = []
        for row in data:
            rows.append(f"({row['clouds']}, '{row['forecast']}', {row['wind_speed']}, '{row['location_name']}', {row['temp']})")

        query = f"""
        INSERT INTO `{table_id}` (clouds, forecast, wind_speed, location_name, temp)
        VALUES {', '.join(rows)}
        """
        query_job = client.query(query)
        query_job.result()
        print('new rows have been added!')
    except Exception as e:
        print(f'Error sending data to BigQuery: {e}')

# Loads the main page
@app.route('/', methods=['GET'])
def index():
    check_if_dataset_exists()
    check_if_table_exists()
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
        lat = request.args.get('lat')
        lon = request.args.get('lon')
        url = f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}'
        data = requests.get(url)
        weather_data = data.json()

        # Send entire json response to BigQuery
        try:
            send_data_to_bigquery(weather_data)
        except Exception as e:
            app.logger.exception(f'Error sending data to BigQuery: {e}')
            return redirect(url_for('error', message=f'Error sending data to BigQuery: {e}'))

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
        weather_data = session.get('weather_data')
        clouds = weather_data.get('clouds', {}).get('all', 'N/A')
        forecast = weather_data.get('weather', {})[0].get('description', 'N/A')
        wind_speed = weather_data.get('wind').get('speed', 'N/A')
        location = weather_data.get('name', 'N/A')
        temp = weather_data.get('main', {}).get('feels_like', 'N/A')
        temp = (temp - 273.15)
        temp = f'{temp:.2f}'

        data = [weather_data, clouds, forecast, wind_speed, location, temp]

        # Creates a database connection. The use of with will shut close the connection incase of an error
        # with Database() as db_connection:
        #     try:
        #         db_connection.create_table()
        #         print("TABLE CREATED")
        #         # for obj in data:
        #         if not data or data == 'N/A':
        #             raise ValueError(f'Missing or invalid data: {data}')
        #         data_obj = {
        #             'clouds': clouds,
        #             'forecast': forecast,
        #             'wind_speed': wind_speed,
        #             'location_name': location,
        #             'temp': temp
        #         }
        #         db_connection.save_data(data_obj)
        #     except ValueError as e:
        #         app.logger.exception(f'ValueError: {e}')
        #         return redirect(url_for('error', message=f'ValueError: {e}'))
                
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
