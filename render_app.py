import os
import json
import threading
from flask import Flask, jsonify
import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# Inisialisasi Flask Web Server
app = Flask(__name__)

# --- KONFIGURASI INFLUXDB CLOUD ---
INFLUX_URL = "https://us-east-1-1.aws.cloud2.influxdata.com"
INFLUX_TOKEN = os.environ.get("INFLUX_TOKEN")

if not INFLUX_TOKEN:
    raise ValueError("FATAL ERROR: Token InfluxDB tidak ditemukan di Environment Variables!")
INFLUX_ORG = "ITS"
INFLUX_BUCKET = "arifin_iot_db"

influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)

# --- KONFIGURASI MQTT ---
BROKER = "broker.hivemq.com"
PORT = 8883
TOPIC = "dt/arifin-its-2026/telemetry"

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("[MQTT] Terhubung ke HiveMQ Cloud Broker!")
        client.subscribe(TOPIC)
    else:
        print(f"[MQTT] Gagal terhubung, rc={rc}")

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode('utf-8'))
        
        point = Point("environment_monitoring").tag("device", data.get("device_id", "unknown"))
        for sensor in data.get("data", []):
            point.field(sensor.get("sensor"), float(sensor.get("value")))

        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
        print(f" -> [INFLUXDB] Data Seq {data.get('seq_num')} disimpan!")

    except Exception as e:
        print(f"[ERROR] {e}")

# --- FUNGSI MQTT BERJALAN DI LATAR BELAKANG ---
def start_mqtt():
    mqtt_client = mqtt.Client(client_id="Python-Render-Arifin")
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.tls_set()
    mqtt_client.tls_insecure_set(True)
    mqtt_client.connect(BROKER, PORT, 60)
    mqtt_client.loop_forever()

# --- ENDPOINT WEB UNTUK PING (ANTI-TIDUR) ---
@app.route('/health')
def health_check():
    return jsonify({"status": "Aplikasi MQTT & Flask aktif", "author": "Arifin ITS"})

if __name__ == "__main__":
    # Jalankan MQTT di thread terpisah agar tidak memblokir Flask
    mqtt_thread = threading.Thread(target=start_mqtt)
    mqtt_thread.daemon = True
    mqtt_thread.start()

    # Jalankan Flask Web Server
    # Render menggunakan port yang didefinisikan oleh environment variable PORT
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)