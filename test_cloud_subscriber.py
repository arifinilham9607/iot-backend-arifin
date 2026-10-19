import json
import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# --- KONFIGURASI INFLUXDB CLOUD ---
INFLUX_URL = "https://us-east-1-1.aws.cloud2.influxdata.com"
INFLUX_TOKEN = "8-xvoDZWfhX0yD5b88ZcAqdf0oq888FTO606e3E3DYM-tfc6g5M8ByFUYTcwsekEzFuEja6ASjfSUNfEpxq73w=="
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
        payload_str = msg.payload.decode('utf-8')
        data = json.loads(payload_str)
        seq = data.get("seq_num")
        print(f"[DATA MASUK] Memproses Seq: {seq}")

        # Parsing JSON ke format Point InfluxDB
        device_id = data.get("device_id", "unknown_device")
        sensors = data.get("data", [])

        point = Point("environment_monitoring").tag("device", device_id)
        
        for sensor in sensors:
            sensor_name = sensor.get("sensor")
            sensor_value = float(sensor.get("value"))
            point.field(sensor_name, sensor_value)

        # Eksekusi penyimpanan ke InfluxDB Cloud
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
        print(" -> [INFLUXDB] Data berhasil disimpan ke Cloud!")

    except Exception as e:
        print(f"[ERROR] Kegagalan sistem saat memproses data: {e}")

mqtt_client = mqtt.Client(client_id="Python-Cloud-Ingestor-Arifin")
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.tls_set()
mqtt_client.tls_insecure_set(True)

print("Memulai Cloud Ingestor...")
mqtt_client.connect(BROKER, PORT, 60)
mqtt_client.loop_forever()