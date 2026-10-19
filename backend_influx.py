import json
import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient

# --- KONFIGURASI JARINGAN ---
MQTT_BROKER = "localhost"  # IP adapter hotspot Windows-mu
MQTT_PORT = 1883
MQTT_TOPIC = "dt/arifin-node-01/telemetry"

# --- KONFIGURASI INFLUXDB ---
INFLUX_HOST = "localhost"
INFLUX_PORT = 8086
INFLUX_DB = "lab_telemetry"

# Inisialisasi klien InfluxDB
db_client = InfluxDBClient(host=INFLUX_HOST, port=INFLUX_PORT)

# Buat database jika belum ada
db_client.create_database(INFLUX_DB)
db_client.switch_database(INFLUX_DB)
print(f"Terhubung ke InfluxDB. Target Database: {INFLUX_DB}")

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"Terhubung ke MQTT Broker! Berlangganan topik: {MQTT_TOPIC}")
        client.subscribe(MQTT_TOPIC)
    else:
        print(f"Gagal terhubung ke MQTT Broker, kode error: {rc}")

def on_message(client, userdata, msg):
    payload = msg.payload.decode('utf-8')
    try:
        data = json.loads(payload)
        device_id = data.get("device_id", "unknown")
        
        influx_points = []
        
        # 1. Mengekstrak Data Sensor Inti
        for item in data.get("data", []):
            point = {
                "measurement": "sensor_telemetry",
                "tags": {
                    "device_id": device_id,
                    "sensor": item["sensor"],
                    "unit": item["unit"]
                },
                "fields": {
                    "value": float(item["value"])
                }
            }
            influx_points.append(point)
        
        # 2. Mengekstrak Status Sistem (Uptime, dll)
        sys_data = data.get("sys", {})
        if sys_data:
            sys_point = {
                "measurement": "system_status",
                "tags": {
                    "device_id": device_id,
                    "fw_ver": sys_data.get("fw_ver")
                },
                "fields": {
                    "uptime_s": int(sys_data.get("uptime_s", 0)),
                    "fan_speed": int(sys_data.get("fan_speed", 0))
                }
            }
            influx_points.append(sys_point)
            
        # 3. Menulis Data ke InfluxDB
        db_client.write_points(influx_points)
        print(f"[{data.get('seq_num')}] Data berhasil ditulis ke InfluxDB: {len(influx_points)} metrik")
        
    except json.JSONDecodeError:
        print("Error: Payload tidak menggunakan format JSON yang valid.")
    except Exception as e:
        print(f"Error kritikal saat memproses data: {e}")

# Inisialisasi Klien MQTT
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

print("Menghubungi MQTT Broker...")
client.connect(MQTT_BROKER, MQTT_PORT, 60)

# Jalankan proses pendengar secara permanen
try:
    client.loop_forever()
except KeyboardInterrupt:
    print("\nProses dihentikan oleh pengguna.")
    client.disconnect()