from flask import Flask, request
import psycopg2
import csv
from io import StringIO
import os

app = Flask(_name_)

# PostgreSQL connection parameters
DB_HOST = os.getenv('DB_HOST', '107.22.69.183')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'iotbd')
DB_USER = os.getenv('DB_USER', 'iotbd')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'rre7E9k24J1Z')

# Initialize PostgreSQL database
def init_db():
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS freefall_data (
        session_id INTEGER,
        timestamp BIGINT,
        accelX REAL,
        accelY REAL,
        accelZ REAL,
        gyroX REAL,
        gyroY REAL,
        gyroZ REAL,
        posX REAL,
        posY REAL,
        posZ REAL
    )''')
    conn.commit()
    conn.close()

@app.route('/freefall', methods=['POST'])
def receive_data():
    try:
        # Get CSV data from POST request
        csv_data = request.data.decode('utf-8')
        csv_reader = csv.reader(StringIO(csv_data))
        
        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        c = conn.cursor()
        
        # Skip header row
        next(csv_reader)
        
        # Insert each row into the database
        for row in csv_reader:
            if len(row) == 11:  # Ensure correct number of columns
                c.execute('''INSERT INTO freefall_data (
                    session_id, timestamp, accelX, accelY, accelZ,
                    gyroX, gyroY, gyroZ, posX, posY, posZ
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
                (
                    int(row[0]), int(row[1]),
                    float(row[2]), float(row[3]), float(row[4]),
                    float(row[5]), float(row[6]), float(row[7]),
                    float(row[8]), float(row[9]), float(row[10])
                ))
        
        conn.commit()
        conn.close()
        
        return "Data stored successfully", 200
    except Exception as e:
        return f"Error: {str(e)}", 500

if _name_ == '_main_':
    #init_db()
    app.run(host='0.0.0.0', port=5000)