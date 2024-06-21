from dotenv import load_dotenv
import os
import psycopg2

load_dotenv()
db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASS')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')

class Database:
    def __init__(self):
        self.conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_pass,
            host=db_host,
            port=db_port
        )
        self.cursor = self.conn.cursor()
        
    
    def create_table(self):
        try:
            table_creation = '''
                CREATE TABLE IF NOT EXISTS weather_data (
                    id SERIAL PRIMARY KEY,
                    location_name TEXT NOT NULL,
                    wind_speed DOUBLE PRECISION,
                    temp DOUBLE PRECISION,
                    forecast TEXT NOT NULL,
                    clouds TEXT NOT NULL
                )
            '''
            self.cursor.execute(table_creation)
            self.conn.commit()
            print("Table created or already exists.")
        except Exception as e:
            print(f"Error creating table: {e}")

    def save_data(self, data):
        insert_query = "INSERT INTO weather_data (clouds, forecast, wind_speed, location_name, temp) VALUES (%s, %s, %s, %s, %s)"
        self.cursor.execute(insert_query, (data['clouds'], data['forecast'], data['wind_speed'], data['location_name'], data['temp']))
        self.conn.commit()
        print("Saved to database")

        
    def close(self):
        self.cursor.close()
        self.conn.close()

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()



        