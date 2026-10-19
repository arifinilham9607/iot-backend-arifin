import os
import json
import threading
from flask import Flask, jsonify
from flask_cors import CORS
import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

app = Flask(__name__)
CORS(app) # Wajib untuk mengizinkan Vercel membaca API

INFLUX_URL = "https://us-east-1-1.aws.cloud2.influxdata.com"
INFLUX_TOKEN = os.environ.get("INFLUX_TOKEN")
INFLUX_ORG = "ITS"
INFLUX_BUCKET = "arifin_iot_db"

if not INFLUX_TOKEN:
    print("WARNING: Token InfluxDB tidak ditemukan. Data tidak akan tersimpan.")

influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)
query_api = influx_client.query_api()

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

        if INFLUX_TOKEN:
            write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
            print(f" -> [INFLUXDB] Data Seq {data.get('seq_num')} disimpan!")
    except Exception as e:
        print(f"[ERROR] {e}")

def start_mqtt():
    mqtt_client = mqtt.Client(client_id="Python-Render-API")
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.tls_set()
    mqtt_client.tls_insecure_set(True)
    mqtt_client.connect(BROKER, PORT, 60)
    mqtt_client.loop_forever()

@app.route('/health')
def health_check():
    return jsonify({"status": "Aplikasi MQTT & Flask aktif", "author": "Arifin ITS"})

# --- ENDPOINT BARU UNTUK VERCEL ---
@app.route('/api/data')
def get_data():
    try:
        # Mengambil 10 data terakhir dari InfluxDB
        query = f'''
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -1h)
          |> filter(fn: (r) => r["_measurement"] == "environment_monitoring")
          |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
          |> sort(columns: ["_time"], desc: true)
          |> limit(n: 10)
        '''
        tables = query_api.query(query, org=INFLUX_ORG)

        results = []
        for table in tables:
            for record in table.records:
                results.append({
                    "time": record.get_time().strftime("%Y-%m-%d %H:%M:%S"),
                    "device": record.values.get("device"),
                    "dht_temp": record.values.get("dht_temp"),
                    "dht_hum": record.values.get("dht_hum"),
                    "sharp_dust": record.values.get("sharp_dust")
                })
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    mqtt_thread = threading.Thread(target=start_mqtt)
    mqtt_thread.daemon = True
    mqtt_thread.start()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)