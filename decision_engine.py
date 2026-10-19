import json
import time
import threading
import uuid
import paho.mqtt.client as mqtt

BROKER = "localhost"
PORT = 1883
DATA_TOPIC = "dt/arifin-node-01/telemetry"
CMD_TOPIC = "cmd/arifin-node-01/action"

# Parameter Baru (Tata Udara Laboratorium)
RH_PENALTY_THRESHOLD = 75.0 # Batas persentase kelembapan di mana pembacaan debu mulai diragukan
DUST_SAFE_LIMIT = 0.15 # mg/m3
DUST_WARN_LIMIT = 0.30 # mg/m3

CLOUD_FAILSAFE_TIMEOUT = 15
DEBOUNCE_LIMIT = 3

# State
last_valid_data_time = time.time()
last_seen_seq = 0
engine_assumed_speed = 0 # Persentase PWM Kipas (0-100)

consecutive_count = 0
pending_target_speed = 0

def send_fan_command(client, speed, reason):
    global engine_assumed_speed
    cmd_id = str(uuid.uuid4())[:8]
    engine_assumed_speed = speed
    
    payload = {
        "cmd_id": cmd_id,
        "command": "FAN_SET",
        "speed_percent": speed,
        "reason": reason,
        "timestamp_ms": int(time.time() * 1000)
    }
    client.publish(CMD_TOPIC, json.dumps(payload), qos=1, retain=True)
    print(f"[ENGINE] -> TX CMD: FAN_SET {speed}% | Alasan: {reason} | ID: {cmd_id}")

def on_connect(client, userdata, flags, rc):
    print(f"[ENGINE] Terhubung ke Broker. Kode: {rc}")
    client.subscribe(DATA_TOPIC, qos=1)

def on_message(client, userdata, msg):
    global last_valid_data_time, last_seen_seq, consecutive_count, pending_target_speed
    
    try:
        payload = json.loads(msg.payload.decode())
        
        current_seq = payload.get("seq_num", 0)
        if current_seq <= last_seen_seq and current_seq != 1:
            print(f"[ENGINE-WARN] Paket Duplikat diabaikan: Seq {current_seq}.")
            return
        elif current_seq > last_seen_seq + 1 and last_seen_seq != 0:
            print(f"[ENGINE-WARN] Packet Loss terdeteksi! Hilang {current_seq - last_seen_seq - 1} paket.")
            
        last_seen_seq = current_seq
        if payload.get("status") == "error_timeout": return

        # Ekstraksi Multi-Sensor
        dust = None
        temp = None
        hum = None
        
        for item in payload.get("data", []):
            if item.get("sensor") == "sharp_dust" and item.get("quality") == "ok":
                dust = item.get("value")
            elif item.get("sensor") == "dht_temp" and item.get("quality") == "ok":
                temp = item.get("value")
            elif item.get("sensor") == "dht_hum" and item.get("quality") == "ok":
                hum = item.get("value")
                
        if dust is None or hum is None:
            print("[ENGINE] Data fusi tidak lengkap atau sensor cacat. Abaikan evaluasi.")
            return

        last_valid_data_time = time.time()
        reported_speed = payload.get("sys", {}).get("fan_speed", 0)

        # ---------------------------------------------------------
        # ALGORITMA FUSI SENSOR: Kompensasi Kelembapan (RH Penalty)
        # ---------------------------------------------------------
        compensated_dust = dust
        confidence = "HIGH"
        
        if hum > RH_PENALTY_THRESHOLD:
            # Jika RH tinggi, pantulan uap air dianggap debu oleh optik Sharp. 
            # Kita potong nilai debu sebesar 40% agar tidak panik.
            compensated_dust = dust * 0.6
            confidence = "LOW (RH Penalty)"

        # Penentuan Kecepatan Kipas Berbasis Aturan Lapis
        target_speed = 0
        if compensated_dust > DUST_WARN_LIMIT:
            target_speed = 100
            eval_reason = "kritis_polusi_tinggi"
        elif compensated_dust > DUST_SAFE_LIMIT:
            target_speed = 50
            eval_reason = "peringatan_polusi_sedang"
        else:
            target_speed = 0
            eval_reason = "udara_bersih"

        print(f"[EVAL] Debu Mentah: {dust:.2f} | RH: {hum:.1f}% | Debu Terkompensasi: {compensated_dust:.2f} ({confidence}) -> Target: {target_speed}%")

        # Histeresis Waktu (Debounce)
        if target_speed == pending_target_speed:
            consecutive_count += 1
        else:
            consecutive_count = 1
            pending_target_speed = target_speed

        if consecutive_count >= DEBOUNCE_LIMIT and target_speed != engine_assumed_speed:
            send_fan_command(client, target_speed, eval_reason)

        # Pemulihan dari Failsafe Perangkat
        if payload.get("sys", {}).get("actuator_state") == "SAFE_MODE" and target_speed == 0:
            send_fan_command(client, 0, "cloud_recovery_from_safe_mode")
            
    except Exception as e:
        print(f"[ENGINE-ERR] Gagal parsing logika: {e}")

def cloud_failsafe_monitor(client):
    global engine_assumed_speed # FIX ERROR UNBOUND LOCAL VARIABLE
    last_heartbeat_time = time.time()
    while True:
        time.sleep(2)
        if time.time() - last_valid_data_time > CLOUD_FAILSAFE_TIMEOUT:
            if engine_assumed_speed != -1:
                print("[ENGINE] !!! KRITIS: Koneksi Edge hilang. Memancarkan perintah Failsafe.")
                send_fan_command(client, 20, "cloud_failsafe_timeout") # Putar kipas lambat sebagai pengaman minimum
                engine_assumed_speed = -1 
        else:
            if time.time() - last_heartbeat_time > 10 and engine_assumed_speed != -1:
                send_fan_command(client, engine_assumed_speed, "heartbeat_sync")
                last_heartbeat_time = time.time()

if __name__ == "__main__":
    client = mqtt.Client(client_id="decision_engine_01")
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(BROKER, PORT, 60)
    
    threading.Thread(target=cloud_failsafe_monitor, args=(client,), daemon=True).start()
    client.loop_forever()