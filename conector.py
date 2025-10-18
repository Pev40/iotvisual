from flask import Flask, request
import psycopg2
import csv
from io import StringIO
import os
import logging

# Configurar logging con flush inmediato
import sys
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout,
    force=True
)
logger = logging.getLogger(__name__)

# Forzar flush inmediato de todos los logs
for handler in logging.root.handlers:
    handler.setLevel(logging.INFO)
    handler.flush = lambda: sys.stdout.flush()

app = Flask(__name__)

# Configurar logging de Flask
@app.after_request
def after_request(response):
    logger.info(f"📤 Respuesta: {response.status} - {request.method} {request.path}")
    return response

#postgresql://iotbd:SoosIOT@52.90.165.189:5432/iotbd
# PostgreSQL connection parameters
DB_HOST = os.getenv('DB_HOST', '52.90.165.189')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'iotbd')
DB_USER = os.getenv('DB_USER', 'iotbd')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'SoosIOT')

# Initialize PostgreSQL database
def init_db():
    try:
        logger.info("🔧 Inicializando base de datos...")
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        c = conn.cursor()
        
        # Crear tabla si no existe
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
        
        # Verificar esquema de la tabla
        c.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'freefall_data'
            ORDER BY ordinal_position
        """)
        columns = c.fetchall()
        logger.info("📋 Esquema de tabla freefall_data:")
        for col_name, col_type in columns:
            logger.info(f"   - {col_name}: {col_type}")
        
        conn.close()
        logger.info("✅ Base de datos inicializada correctamente")
        return True
    except Exception as e:
        logger.error(f"❌ Error inicializando base de datos: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False

@app.route('/', methods=['GET'])
def index():
    return {
        "service": "IoT Visual API",
        "version": "1.0.0",
        "endpoints": {
            "POST /freefall": "Recibir datos de sensores (CSV)",
            "GET /health": "Verificar estado del servicio y base de datos"
        }
    }, 200

@app.route('/health', methods=['GET'])
def health():
    logger.info("🏥 Health check solicitado")
    
    health_status = {
        "status": "ok",
        "service": "iotvisual-api",
        "database": "disconnected"
    }
    
    # Verificar conexión a PostgreSQL
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            connect_timeout=3
        )
        conn.close()
        health_status["database"] = "connected"
        logger.info("✅ Health check: Base de datos OK")
        return health_status, 200
    except Exception as e:
        health_status["status"] = "degraded"
        health_status["database_error"] = str(e)
        logger.error(f"❌ Health check: Error en base de datos - {str(e)}")
        return health_status, 503

@app.route('/freefall', methods=['POST'])
def receive_data():
    conn = None
    try:
        client_ip = request.remote_addr
        logger.info(f"📥 Recibiendo datos en /freefall desde {client_ip}")
        sys.stdout.flush()
        
        # Get CSV data from POST request
        try:
            # Log de headers para diagnóstico
            logger.info(f"📋 Headers recibidos: {dict(request.headers)}")
            logger.info(f"📏 Content-Length: {request.content_length}")
            logger.info(f"📝 Content-Type: {request.content_type}")
            sys.stdout.flush()
            
            logger.info("🔄 Intentando leer datos en chunks...")
            sys.stdout.flush()
            
            # Leer datos en chunks para evitar bloqueo
            if request.content_length and request.content_length > 0:
                logger.info(f"📦 Leyendo {request.content_length} bytes en chunks...")
                sys.stdout.flush()
                
                chunks = []
                bytes_to_read = request.content_length
                chunk_size = 8192  # 8KB chunks
                bytes_read = 0
                
                while bytes_read < bytes_to_read:
                    chunk_to_read = min(chunk_size, bytes_to_read - bytes_read)
                    chunk = request.stream.read(chunk_to_read)
                    if not chunk:
                        logger.warning(f"⚠️ Stream terminó prematuramente en {bytes_read}/{bytes_to_read} bytes")
                        sys.stdout.flush()
                        break
                    chunks.append(chunk)
                    bytes_read += len(chunk)
                    if bytes_read % (chunk_size * 4) == 0:  # Log cada 32KB
                        logger.info(f"📊 Progreso: {bytes_read}/{bytes_to_read} bytes ({100*bytes_read//bytes_to_read}%)")
                        sys.stdout.flush()
                
                raw_data = b''.join(chunks)
                logger.info(f"✅ Lectura completa: {len(raw_data)} bytes")
                sys.stdout.flush()
            else:
                logger.info("📦 Sin Content-Length, usando request.data...")
                sys.stdout.flush()
                raw_data = request.data
            
            logger.info(f"📦 Datos obtenidos exitosamente!")
            sys.stdout.flush()
            
            logger.info(f"📏 Tamaño raw: {len(raw_data)} bytes, tipo: {type(raw_data)}")
            sys.stdout.flush()
            
            logger.info("🔄 Intentando decodificar UTF-8...")
            sys.stdout.flush()
            
            csv_data = raw_data.decode('utf-8', errors='replace')
            logger.info(f"✅ Decodificación exitosa!")
            sys.stdout.flush()
            
            logger.info(f"📊 Tamaño de texto CSV: {len(csv_data)} bytes")
            sys.stdout.flush()
            
            # Imprimir primeras líneas
            lines = csv_data.split('\n')
            logger.info(f"📄 Total de líneas: {len(lines)}")
            logger.info(f"🔍 Primeras 5 líneas:")
            for i, line in enumerate(lines[:5]):
                logger.info(f"  Línea {i}: {line[:100]}")
            sys.stdout.flush()
            
        except Exception as e:
            logger.error(f"❌ Error al decodificar datos: {str(e)}")
            sys.stdout.flush()
            import traceback
            logger.error(traceback.format_exc())
            sys.stdout.flush()
            return f"Error al decodificar datos: {str(e)}", 400
        
        # Verificar que hay datos
        if not csv_data or len(csv_data) == 0:
            logger.warning("⚠️ No se recibieron datos")
            sys.stdout.flush()
            return "No data received", 400
        
        logger.info("📋 Creando CSV reader...")
        sys.stdout.flush()
        csv_reader = csv.reader(StringIO(csv_data))
        logger.info("✅ CSV reader creado")
        sys.stdout.flush()
        
        # Connect to PostgreSQL database
        logger.info("🔌 Conectando a PostgreSQL...")
        sys.stdout.flush()
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                connect_timeout=5
            )
            c = conn.cursor()
            logger.info("✅ Conexión exitosa a la base de datos")
            sys.stdout.flush()
        except Exception as e:
            logger.error(f"❌ Error conectando a PostgreSQL: {str(e)}")
            sys.stdout.flush()
            import traceback
            logger.error(traceback.format_exc())
            sys.stdout.flush()
            return f"Database connection error: {str(e)}", 500
        
        # Skip header row
        logger.info("📋 Procesando header del CSV...")
        sys.stdout.flush()
        try:
            next(csv_reader)
            logger.info("✅ Header del CSV procesado")
            sys.stdout.flush()
        except StopIteration:
            logger.warning("⚠️ CSV vacío o sin header")
            sys.stdout.flush()
            if conn:
                conn.close()
            return "Empty CSV", 400
        
        # Insert each row into the database
        logger.info("🔄 Iniciando inserción de filas...")
        sys.stdout.flush()
        row_count = 0
        skipped_rows = 0
        error_rows = []
        
        for idx, row in enumerate(csv_reader, start=2):  # start=2 porque header es línea 1
            if len(row) != 11:
                logger.warning(f"⚠️ Fila {idx} omitida: tiene {len(row)} columnas en lugar de 11")
                skipped_rows += 1
                continue
            
            try:
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
                # Commit cada fila individualmente para evitar que un error aborte todas
                conn.commit()
                row_count += 1
            except Exception as e:
                # Rollback de la transacción abortada
                conn.rollback()
                error_msg = str(e)
                logger.error(f"❌ Error insertando fila {idx}: {error_msg}")
                logger.error(f"   Datos de la fila: {row}")
                # Si es el primer error, mostrar más detalles
                if len(error_rows) == 0:
                    logger.error(f"   ⚠️ PRIMER ERROR - Detalles completos:")
                    logger.error(f"   session_id={row[0]} ({type(row[0])})")
                    logger.error(f"   timestamp={row[1]} ({type(row[1])})")
                    import traceback
                    logger.error(traceback.format_exc())
                sys.stdout.flush()
                error_rows.append(idx)
        
        logger.info(f"💾 Cerrando conexión...")
        sys.stdout.flush()
        conn.close()
        logger.info("✅ Conexión cerrada")
        sys.stdout.flush()
        
        logger.info(f"✅ {row_count} filas insertadas exitosamente")
        if skipped_rows > 0:
            logger.warning(f"⚠️ {skipped_rows} filas omitidas (columnas incorrectas)")
        if error_rows:
            logger.error(f"❌ Errores en filas: {error_rows}")
        sys.stdout.flush()
        
        return {
            "status": "success",
            "rows_inserted": row_count,
            "rows_skipped": skipped_rows,
            "rows_with_errors": len(error_rows)
        }, 200
        
    except Exception as e:
        logger.error(f"❌ Error inesperado: {str(e)}", exc_info=True)
        sys.stdout.flush()
        import traceback
        logger.error(traceback.format_exc())
        sys.stdout.flush()
        if conn:
            try:
                conn.close()
            except:
                pass
        return f"Error: {str(e)}", 500

if __name__ == '__main__':
    # Inicializar base de datos
    if init_db():
        logger.info("🚀 Iniciando servidor Flask en puerto 5000")
        logger.info("📡 Esperando datos en POST /freefall")
        logger.info("⚙️  Modo threaded habilitado para mejor manejo de conexiones")
        app.run(host='0.0.0.0', port=5000, threaded=True)
    else:
        logger.error("❌ No se pudo inicializar la base de datos. Abortando.")
        sys.exit(1)
