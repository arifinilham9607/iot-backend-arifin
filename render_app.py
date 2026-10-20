import os
import json
import threading
import uuid
from datetime import timedelta
from flask import Flask, jsonify
from flask_cors import CORS
import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

app = Flask(__name__)
CORS(app)

# Konfigurasi InfluxDB
INFLUX_URL = "https://us-east-1-1.aws.cloud2.influxdata.com"
INFLUX_TOKEN = os.environ.get("INFLUX_TOKEN")
INFLUX_ORG = "ITS"
INFLUX_BUCKET = "arifin_iot_db"

influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)
query_api = influx_client.query_api()

# Konfigurasi MQTT
BROKER = "broker.hivemq.com"
PORT = 8883
TOPIC = "dt/arifin-its-2026/telemetry"

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("[MQTT] Terhubung ke HiveMQ Cloud Broker!", flush=True)
        client.subscribe(TOPIC)
    else:
        print(f"[MQTT] Gagal terhubung, rc={rc}", flush=True)

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode('utf-8'))
        point = Point("environment_monitoring").tag("device", data.get("device_id", "unknown"))
        
        # Validasi aman untuk nilai sensor null / None
        for sensor in data.get("data", []):
            val = sensor.get("value")
            sensor_name = sensor.get("sensor")
            if val is not None and sensor_name is not None:
                try:
                    point.field(sensor_name, float(val))
                except (ValueError, TypeError):
                    pass

        if INFLUX_TOKEN:
            write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
            print(f" -> [INFLUXDB] Data Seq {data.get('seq_num')} disimpan!", flush=True)
    except Exception as e:
        print(f"[ERROR] {e}", flush=True)

def start_mqtt():
    unique_client_id = f"Python-Render-API-{uuid.uuid4().hex[:6]}"
    mqtt_client = mqtt.Client(client_id=unique_client_id)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.tls_set()
    mqtt_client.tls_insecure_set(True)
    mqtt_client.connect(BROKER, PORT, 60)
    mqtt_client.loop_forever()

# Inisialisasi Thread MQTT
mqtt_thread = threading.Thread(target=start_mqtt)
mqtt_thread.daemon = True
mqtt_thread.start()

@app.route('/health')
def health_check():
    return jsonify({"status": "Aplikasi MQTT & Flask aktif", "author": "Arifin ITS"})

@app.route('/api/data')
def get_data():
    try:
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
                # Konversi waktu UTC dari InfluxDB ke WIB (UTC + 7 jam)
                wib_time = record.get_time() + timedelta(hours=7)
                
                results.append({
                    "time": wib_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "device": record.values.get("device"),
                    "dht_temp": record.values.get("dht_temp"),
                    "dht_hum": record.values.get("dht_hum"),
                    "sharp_dust": record.values.get("sharp_dust")
                })
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)