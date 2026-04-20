import json
import time
import ssl
import os
import psycopg2
import paho.mqtt.client as mqtt

MQTT_BROKER = os.environ["MQTT_BROKER"]
MQTT_PORT   = int(os.environ.get("MQTT_PORT", 8883))
MQTT_USER   = os.environ["MQTT_USER"]
MQTT_PASS   = os.environ["MQTT_PASS"]

DB_CONFIG = {
    "host":     os.environ["DB_HOST"],
    "port":     int(os.environ.get("DB_PORT", 6543)),
    "dbname":   os.environ.get("DB_NAME", "postgres"),
    "user":     os.environ["DB_USER"],
    "password": os.environ["DB_PASS"]
}

db = psycopg2.connect(**DB_CONFIG)
db.autocommit = True
cursor = db.cursor()
print("Conectado a Supabase")

current = {
    "tempC": None, "humPct": None, "dhtFails": 0,
    "heater": 0, "humid": 0, "extractor": 0, "fan": 0,
    "mode": 0, "pidPct": 0, "spTemp": 0, "spHum": 0
}

MODE_MAP = {"NORMAL": 0, "PREHEAT": 1, "ALARM": 2}
last_insert_time = 0
INSERT_INTERVAL  = 10

def try_insert():
    global last_insert_time
    if current["tempC"] is None or current["humPct"] is None:
        return
    now = time.time()
    if now - last_insert_time < INSERT_INTERVAL:
        return
    last_insert_time = now
    try:
        cursor.execute(
            "INSERT INTO lecturas "
            "(temp_c, hum_pct, sp_temp, sp_hum, heater, humid, "
            " extractor, fan, pid_pct, dht_fails, mode) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (current["tempC"], current["humPct"],
             current["spTemp"], current["spHum"],
             current["heater"], current["humid"],
             current["extractor"], current["fan"],
             current["pidPct"], current["dhtFails"],
             current["mode"])
        )
        print(f"[OK] T={current['tempC']}°C H={current['humPct']}%")
    except psycopg2.Error as e:
        print(f"[ERROR] DB: {e}")
    current["tempC"] = None
    current["humPct"] = None

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        client.subscribe("HMI_automat/incubadora_01/#")
        print("MQTT conectado")
    else:
        print(f"Error MQTT rc={rc}")

def on_message(client, userdata, msg):
    topic = msg.topic
    try:
        d = json.loads(msg.payload.decode())
    except:
        return
    if topic.endswith("/sensores"):
        current["tempC"]    = d.get("t")
        current["humPct"]   = d.get("h")
        current["dhtFails"] = d.get("fail", 0)
    elif topic.endswith("/actuadores"):
        current["heater"]    = d.get("htR", 0)
        current["humid"]     = d.get("hmd", 0)
        current["extractor"] = d.get("ext", 0)
        current["fan"]       = d.get("fan", 0)
    elif topic.endswith("/control"):
        current["mode"]   = MODE_MAP.get(d.get("mode", "NORMAL"), 0)
        current["pidPct"] = d.get("pid", 0)
        current["spTemp"] = d.get("spT", 0)
        current["spHum"]  = d.get("spH", 0)
        try_insert()

client = mqtt.Client()
client.username_pw_set(MQTT_USER, MQTT_PASS)
client.tls_set(tls_version=ssl.PROTOCOL_TLS)
client.on_connect = on_connect
client.on_message = on_message
client.connect(MQTT_BROKER, MQTT_PORT, 60)

print("Puente iniciado")
# Correr por máximo 5.5 horas (20000 segundos) para no exceder el límite de 6h de GitHub
end_time = time.time() + 20000
while time.time() < end_time:
    client.loop(timeout=1.0)
print("Tiempo completado")
