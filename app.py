from flask import Flask,request, render_template, redirect, url_for, session
import requests
import os
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler
import logging
from db import Database

# Start flask application
app = Flask(__name__)
# Load env variables
load_dotenv()
api_key = os.getenv('API_KEY')
app.secret_key = os.getenv('SECRET_KEY')

# Set up logging
if not app.debug:
    handler = RotatingFileHandler('error.log', maxBytes=10000, backupCount=1)
    handler.setLevel(logging.ERROR)
    app.logger.addHandler(handler)

# Loads the main page
@app.route('/', methods=['GET'])
def index():
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
        geo_url = f'https://api.openweathermap.org/geo/1.0/direct?q={location_name}&limit=1&appid=02853e8a712ec812acbe104740b2b900'
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
        with Database() as db_connection:
            try:
                db_connection.create_table()
                print("TABLE CREATED")
                for obj in data:
                    if not obj or obj == 'N/A':
                        raise ValueError(f'Missing or invalid data: {obj}')
                    data_obj = {
                        'clouds': clouds,
                        'forecast': forecast,
                        'wind_speed': wind_speed,
                        'location_name': location,
                        'temp': temp
                    }
                    db_connection.save_data(data_obj)
            except ValueError as e:
                app.logger.exception(f'ValueError: {e}')
                return redirect(url_for('error', message=f'ValueError: {e}'))
                
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
