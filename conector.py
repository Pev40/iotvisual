from flask import Flask, request
import psycopg2
import csv
from io import StringIO
import os
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
#postgresql://iotbd:SoosIOT@52.90.165.189:5432/iotbd
# PostgreSQL connection parameters
DB_HOST = os.getenv('DB_HOST', '52.90.165.189')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'iotbd')
DB_USER = os.getenv('DB_USER', 'iotbd')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'SoosIOT')

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

@app.route('/health', methods=['GET'])
def health():
    logger.info("🏥 Health check solicitado")
    return {"status": "ok", "message": "Servidor funcionando correctamente"}, 200

@app.route('/freefall', methods=['POST'])
def receive_data():
    try:
        client_ip = request.remote_addr
        logger.info(f"📥 Recibiendo datos en /freefall desde {client_ip}")
        
        # Get CSV data from POST request
        csv_data = request.data.decode('utf-8')
        logger.info(f"📊 Tamaño de datos recibidos: {len(csv_data)} bytes")
        
        csv_reader = csv.reader(StringIO(csv_data))
        
        # Connect to PostgreSQL database
        logger.info("🔌 Conectando a PostgreSQL...")
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        c = conn.cursor()
        logger.info("✅ Conexión exitosa a la base de datos")
        
        # Skip header row
        next(csv_reader)
        
        # Insert each row into the database
        row_count = 0
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
                row_count += 1
        
        conn.commit()
        conn.close()
        
        logger.info(f"✅ {row_count} filas insertadas exitosamente en la base de datos")
        return "Data stored successfully", 200
    except Exception as e:
        logger.error(f"❌ Error al procesar datos: {str(e)}")
        return f"Error: {str(e)}", 500

if __name__ == '__main__':
    #init_db()
    logger.info("🚀 Iniciando servidor Flask en puerto 5000")
    logger.info("📡 Esperando datos en POST /freefall")
    app.run(host='0.0.0.0', port=5000)
