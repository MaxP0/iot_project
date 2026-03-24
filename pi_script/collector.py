import json
import time
import sqlite3
import logging
from datetime import datetime
from sense_hat import SenseHat
import paho.mqtt.client as mqtt

#  Logging setup 
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
log = logging.getLogger(__name__)

#  Load config 
def load_config(path='/home/nci/IoT-Project/config/config.json'):
    with open(path, 'r') as f:
        return json.load(f)

#  Database 
def init_db(db_path):
    conn = sqlite3.connect(db_path)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS sensor_readings (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp   TEXT    NOT NULL,
            device_id   TEXT    NOT NULL,
            latitude    REAL,
            longitude   REAL,
            temperature REAL,
            humidity    REAL,
            pressure    REAL,
            colour_r    INTEGER,
            colour_g    INTEGER,
            colour_b    INTEGER,
            colour_c    INTEGER,
            pitch       REAL,
            roll        REAL,
            yaw         REAL
        )
    ''')
    conn.commit()
    log.info("Database initialised at %s", db_path)
    return conn

def save_to_db(conn, reading):
    conn.execute('''
        INSERT INTO sensor_readings (
            timestamp, device_id, latitude, longitude,
            temperature, humidity, pressure,
            colour_r, colour_g, colour_b, colour_c,
            pitch, roll, yaw
        ) VALUES (
            :timestamp, :device_id, :latitude, :longitude,
            :temperature, :humidity, :pressure,
            :colour_r, :colour_g, :colour_b, :colour_c,
            :pitch, :roll, :yaw
        )
    ''', reading)
    conn.commit()

#  Sensors 
def getTemperature(sense):
    return round(sense.get_temperature(), 2)

def getHumidity(sense):
    return round(sense.get_humidity(), 2)

def getPressure(sense):
    return round(sense.get_pressure(), 2)

def getColour(sense):
    r, g, b, c = sense.colour.colour
    return {'r': r, 'g': g, 'b': b, 'clarity': c}

def getOrientation(sense):
    o = sense.get_orientation()
    return {
        'pitch': round(o['pitch'], 2),
        'roll':  round(o['roll'],  2),
        'yaw':   round(o['yaw'],   2)
    }

#  LED indicator 
def flashLED(sense, success=True):
    colour = (0, 255, 0) if success else (255, 0, 0)
    sense.clear(colour)
    time.sleep(0.2)
    sense.clear()

#  MQTT callbacks 
def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        log.info("Connected to MQTT broker")
    else:
        log.error("MQTT connection failed: %s", reason_code)

def on_publish(client, userdata, mid, reason_code, properties):
    log.info("Message published (mid=%s)", mid)

#  Main loop 
def main():
    config   = load_config()
    sense    = SenseHat()
    sense.colour.gain = 64
    sense.colour.integration_cycles = 64

    log.info("Warming up sensors...")
    time.sleep(2)

    # Database
    db_conn = init_db(config['database']['path'])

    # MQTT client
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_publish = on_publish
    client.connect(config['mqtt']['broker'], config['mqtt']['port'], keepalive=60)
    client.loop_start()

    log.info("Starting data collection every %ds", config['sample_interval_seconds'])
    sense.show_message("OK", text_colour=(0, 255, 0))

    while True:
        try:
            timestamp   = datetime.now().isoformat()
            colour      = getColour(sense)
            orientation = getOrientation(sense)

            reading = {
                'timestamp':  timestamp,
                'device_id':  config['device_id'],
                'latitude':   config['latitude'],
                'longitude':  config['longitude'],
                'temperature': getTemperature(sense),
                'humidity':    getHumidity(sense),
                'pressure':    getPressure(sense),
                'colour_r':    colour['r'],
                'colour_g':    colour['g'],
                'colour_b':    colour['b'],
                'colour_c':    colour['clarity'],
                'pitch':       orientation['pitch'],
                'roll':        orientation['roll'],
                'yaw':         orientation['yaw'],
            }

            # Save locally
            save_to_db(db_conn, reading)
            log.info("Saved to DB: temp=%.1f  hum=%.1f  pres=%.1f",
                     reading['temperature'], reading['humidity'], reading['pressure'])

            # Publish to cloud
            payload = json.dumps(reading)
            client.publish(config['mqtt']['topic'], payload, qos=1)

            # Flash green = success
            flashLED(sense, success=True)

        except Exception as e:
            log.error("Error: %s", e)
            flashLED(sense, success=False)

        time.sleep(config['sample_interval_seconds'])

if __name__ == '__main__':
    main()
