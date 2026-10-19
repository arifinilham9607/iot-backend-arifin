import json
import paho.mqtt.client as mqtt

BROKER_ADDRESS = "broker.emqx.io"
BROKER_PORT = 1883
TOPIC_TELEMETRY = "dt/arifin-node-01/telemetry"

# Variabel untuk melacak packet loss
last_seq_num = None

def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print("[BACKEND] Berhasil terhubung ke Broker.")
        # Langsung berlangganan (subscribe) ke topik setelah terhubung
        client.subscribe(TOPIC_TELEMETRY, qos=1)
        print(f"[BACKEND] Mendengarkan topik: {TOPIC_TELEMETRY}\n")
    else:
        print(f"[ERROR] Gagal terhubung. Kode: {reason_code}")

def on_message(client, userdata, msg):
    global last_seq_num
    
    try:
        # 1. Dekode payload JSON
        payload_str = msg.payload.decode("utf-8")
        data = json.loads(payload_str)
        
        # 2. Ekstrak metadata
        device_id = data.get("device_id", "Unknown")
        seq_num = data.get("seq_num", 0)
        
        print(f"--- Data Diterima dari {device_id} | Seq: {seq_num} ---")
        
        # 3. Deteksi Packet Loss
        if last_seq_num is not None:
            expected_seq = last_seq_num + 1
            if seq_num > expected_seq:
                hilang = seq_num - expected_seq
                print(f"[WARNING] TERDETEKSI {hilang} PAKET HILANG! (Lompat dari {last_seq_num} ke {seq_num})")
            elif seq_num <= last_seq_num:
                print(f"[WARNING] PAKET DUPLIKAT ATAU OUT-OF-ORDER! (Menerima ulang {seq_num})")
        
        last_seq_num = seq_num

        # 4. Baca Sensor
        sensors = data.get("data", [])
        for s in sensors:
            nama = s.get("sensor")
            nilai = s.get("value")
            satuan = s.get("unit")
            kualitas = s.get("quality")
            
            if kualitas == "ok":
                print(f" > {nama}: {nilai} {satuan}")
            else:
                print(f" > {nama}: [MENGALAMI GANGGUAN - KODE: {kualitas}]")
        print("-" * 45)
        
    except json.JSONDecodeError:
        print(f"[ERROR] Menerima format yang bukan JSON: {msg.payload}")
    except Exception as e:
        print(f"[ERROR] Terjadi kesalahan sistem: {e}")

# Konfigurasi Client MQTT dengan API Versi 2
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="backend_arifin_01")
client.on_connect = on_connect
client.on_message = on_message

print(f"[BACKEND] Menghubungkan ke {BROKER_ADDRESS}...")
client.connect(BROKER_ADDRESS, BROKER_PORT, 60)

try:
    # Memulai loop tak terbatas untuk terus mendengarkan data
    client.loop_forever()
except KeyboardInterrupt:
    print("\n[BACKEND] Dimatikan oleh pengguna.")
finally:
    client.disconnect()