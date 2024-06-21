# VMO2-Onboarding-Project
This project is a simple web application that fetches weather data for a user and stores it in a database.

## Files
### app.py
This is the main file that runs the web application. It uses Flask to create routes and render templates. The main route is /get_weather_for_user, which fetches weather data for a user, stores it in a database, and displays it on a webpage.


The get_weather_for_user function does the following:

Fetches weather data from a session variable.
Extracts relevant information from the weather data.
Creates a database connection.
Creates a table in the database if it doesn't exist.
Saves the weather data to the database.
Renders a webpage with the weather data.
### db.py
This file contains the Database class, which is used to interact with the database. It has two main methods: create_table and save_data.

The create_table method creates a table in the database if it doesn't already exist.

The save_data method saves a given data object to the database.

## How to Run
To run this project, you need to have Python and Flask installed. You can then run the app.py file with Python.

This will start the web server, and you can access the application at http://localhost:5000.

## Dependencies
This project uses the following Python libraries:

Flask
Make sure to install these dependencies using pip:

Future Improvements
Future improvements could include adding more routes, improving error handling, and adding more features to the web interface.
