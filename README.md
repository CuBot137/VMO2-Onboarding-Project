# VM02 Onboarding Project
This project is a Flask-based web application that interacts with the OpenWeatherMap API to fetch weather data for a specified location, uploads the data to Google Cloud Storage (GCS), and loads it into a BigQuery table for further analysis. The application is containerized using Docker.

## Features
Fetch weather data using the OpenWeatherMap API.
Upload the weather data to a Google Cloud Storage bucket.
Load the weather data into a BigQuery table.
Simple web interface to enter location and view weather data.
## Prerequisites
Python 3.11
Docker
Google Cloud SDK with BigQuery and Storage APIs enabled
OpenWeatherMap API key
## Setup
Clone the repository:

git clone https://github.com/yourusername/vm02-onboarding-project.git
cd vm02-onboarding-project
Create and activate a virtual environment:


python -m venv venv
source venv/bin/activate   # On Windows use `venv\Scripts\activate`
Install the dependencies:

pip install -r requirements.txt
Set up environment variables:

Create a .env file in the project root directory with the following content:
makefile
Copy code
API_KEY=your_openweathermap_api_key
SECRET_KEY=your_flask_secret_key
GOOGLE_APPLICATION_CREDENTIALS=/root/service-account-key.json
Download your Google Cloud service account key:

Save the service account key JSON file to a known location, e.g., C:\Users\ConorLynam\Downloads\festive-idea-426808-e6-66b6816fda7c.json.
## Running the Application
python app.py
Access the application:
Open your web browser and go to http://localhost:5000.

## Project Structure
app.py: Main application code.
requirements.txt: List of Python dependencies.
Dockerfile: Docker configuration file.
.env: Environment variables file.
## Key Functions
check_if_bucket_exists(): Checks if the GCS bucket exists, and creates it if not.
create_bucket(): Creates a new GCS bucket.
upload_to_gcs(): Uploads a file to GCS.
load_csv_from_storage_to_bigquery(): Loads a CSV file from GCS to BigQuery.
create_csv_file(): Converts weather data from JSON to CSV.
check_if_dataset_exists(): Checks if the BigQuery dataset exists, and creates it if not.
create_dataset(): Creates a new BigQuery dataset.
## Routes
/: Main page to enter the location.
/error: Error page to display custom error messages.
/location: Processes the user-input location.
/geo_data/<location_name>: Fetches the latitude and longitude of the specified location.
/get_weather: Fetches weather data for the specified location and processes it.
/get_weather_for_user: Displays the weather data to the user.