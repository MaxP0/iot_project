import json
import time
import sqlite3
import logging
import ssl
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
def load_config(path='/home/nci/iot_project/config/config.json'):
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
    """Save a single sensor reading to the local SQLite database."""
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

def animatePublish(sense):
    frames = [
        [(3,3),(4,3),(3,4),(4,4)],
        [(3,2),(4,2),(2,3),(5,3),(2,4),(5,4),(3,5),(4,5)],
        [(3,1),(4,1),(2,2),(5,2),(1,3),(6,3),(1,4),(6,4),(2,5),(5,5),(3,6),(4,6)],
    ]
    for frame in frames:
        sense.clear()
        for x, y in frame:
            sense.set_pixel(x, y, 0, 255, 100)
        time.sleep(0.1)
    sense.clear()

def animateError(sense):
    x_pattern = [
        (0,0),(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),
        (7,0),(6,1),(5,2),(4,3),(3,4),(2,5),(1,6),(0,7)
    ]
    for _ in range(3):
        sense.clear()
        for px, py in x_pattern:
            sense.set_pixel(px, py, 255, 0, 0)
        time.sleep(0.2)
        sense.clear()
        time.sleep(0.2)

#  MQTT callbacks 
def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        log.info("Connected to MQTT broker (TLS)")
    else:
        log.error("MQTT connection failed: %s", reason_code)

def on_publish(client, userdata, mid, reason_code, properties):
    log.info("Message published (mid=%s)", mid)

#  Main loop 
def main():
    config = load_config()
    mqtt_cfg = config['mqtt']

    sense = SenseHat()
    sense.colour.gain = 64
    sense.colour.integration_cycles = 64

    log.info("Warming up sensors...")
    time.sleep(2)

    # Database
    db_conn = init_db(config['database']['path'])

    # MQTT client with TLS
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.username_pw_set(mqtt_cfg['username'], mqtt_cfg['password'])
    client.tls_set(tls_version=ssl.PROTOCOL_TLS_CLIENT)
    client.on_connect = on_connect
    client.on_publish = on_publish
    client.connect(mqtt_cfg['broker'], mqtt_cfg['port'], keepalive=60)
    client.loop_start()

    log.info("Starting data collection every %ds", config['sample_interval_seconds'])
    sense.show_message("OK", text_colour=(0, 255, 0))

    while True:
        try:
            timestamp   = datetime.now().isoformat()
            colour      = getColour(sense)
            orientation = getOrientation(sense)

            reading = {
                'timestamp':   timestamp,
                'device_id':   config['device_id'],
                'latitude':    config['latitude'],
                'longitude':   config['longitude'],
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

            save_to_db(db_conn, reading)
            log.info("Saved to DB: temp=%.1f  hum=%.1f  pres=%.1f",
                     reading['temperature'], reading['humidity'], reading['pressure'])

            payload = json.dumps(reading)
            client.publish(mqtt_cfg['topic'], payload, qos=1)

            animatePublish(sense)


        except Exception as e:
            log.error("Error: %s", e)
            animateError(sense)

        time.sleep(config['sample_interval_seconds'])

if __name__ == '__main__':
    main()