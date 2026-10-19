import json
import time
import random
import threading
import paho.mqtt.client as mqtt

BROKER = "localhost"
PORT = 1883
DATA_TOPIC = "dt/arifin-node-01/telemetry"
CMD_TOPIC = "cmd/arifin-node-01/action"

actual_fan_speed = 0
hardware_status = "NORMAL"
last_cmd_received_time = time.time()
EDGE_FAILSAFE_TIMEOUT = 20

current_dust = 0.05
current_temp = 25.0
current_hum = 60.0

def edge_failsafe_loop():
    global actual_fan_speed, hardware_status
    while True:
        time.sleep(2)
        if time.time() - last_cmd_received_time > EDGE_FAILSAFE_TIMEOUT:
            if hardware_status != "SAFE_MODE":
                print(f"[HW-FAILSAFE] Koneksi terputus > {EDGE_FAILSAFE_TIMEOUT}s. Kipas masuk SAFE_MODE (Putaran 20%).")
                hardware_status = "SAFE_MODE"
                actual_fan_speed = 20 # Kipas berputar rendah agar udara lab tidak benar-benar mati

def on_connect(client, userdata, flags, rc):
    client.subscribe(CMD_TOPIC, qos=1)

def on_message(client, userdata, msg):
    global actual_fan_speed, hardware_status, last_cmd_received_time
    try:
        payload = json.loads(msg.payload.decode())
        command = payload.get("command")
        
        last_cmd_received_time = time.time()
        hardware_status = "NORMAL"
        
        if command == "FAN_SET":
            actual_fan_speed = int(payload.get("speed_percent", 0))
            print(f"[AKTUATOR] Mengeksekusi Putaran Kipas: {actual_fan_speed}%")
            
    except Exception as e:
        print(f"[EDGE-ERR] Gagal membaca perintah: {e}")

client = mqtt.Client(client_id="edge_sim_01")
client.on_connect = on_connect
client.on_message = on_message
client.connect(BROKER, PORT, 60)
client.loop_start()

threading.Thread(target=edge_failsafe_loop, daemon=True).start()

seq_num = 1
print("[SCENARIO] Memulai Loop Tertutup Tata Udara Lab SIS.")

try:
    while True:
        # -----------------------------------------
        # FISIKA TIRUAN LINGKUNGAN TERTUTUP
        # -----------------------------------------
        # Putaran kipas menurunkan debu
        if actual_fan_speed > 0:
            current_dust -= (actual_fan_speed * 0.001)
        else:
            current_dust += random.uniform(0.005, 0.015)
            
        current_dust = max(0.02, min(current_dust, 0.90)) # Batas matematis sensor (mg/m3)
        current_hum += random.uniform(-1.0, 1.0)
        current_hum = max(50.0, min(current_hum, 95.0))
        
        # -----------------------------------------
        # INJEKSI SKENARIO
        # -----------------------------------------
        if 10 <= seq_num <= 15:
            # Skenario 1: Ilusi Uap Air (RH Tinggi memantulkan optik Sharp seolah itu debu)
            if seq_num == 10: print("\n>>> INJEKSI: Ruangan Lembap (Uap). Sensor Sharp tertipu membaca debu tinggi!")
            current_hum = 85.0
            current_dust = 0.25 
        elif seq_num == 20:
            print("\n>>> INJEKSI: Normalisasi Lingkungan")
            current_hum = 60.0
            current_dust = 0.05
        elif 25 <= seq_num <= 30:
            # Skenario 2: Polusi Debu Nyata (RH Normal)
            if seq_num == 25: print("\n>>> INJEKSI: Debu Nyata Menyebar di Laboratorium.")
            current_hum = 60.0
            current_dust = 0.35
            
        payload = {
            "schema_ver": "2.0", # Skema naik versi karena perubahan kontrak
            "device_id": "arifin-node-01",
            "seq_num": seq_num,
            "timestamp_ms": int(time.time() * 1000),
            "data": [
                {"sensor": "sharp_dust", "value": round(current_dust, 3), "unit": "mg/m3", "quality": "ok"},
                {"sensor": "dht_temp", "value": round(current_temp, 1), "unit": "C", "quality": "ok"},
                {"sensor": "dht_hum", "value": round(current_hum, 1), "unit": "%", "quality": "ok"}
            ],
            "sys": {
                "fw_ver": "v2.0-dust-scenario",
                "uptime_s": seq_num * 5,
                "fan_speed": actual_fan_speed,
                "actuator_state": hardware_status
            }
        }
        
        client.publish(DATA_TOPIC, json.dumps(payload), qos=1)
        print(f"[TX] Seq {seq_num:03d} | Debu: {current_dust:0.3f} mg/m3 | RH: {current_hum:0.1f}% | Kipas: {actual_fan_speed}%")
        
        seq_num += 1
        time.sleep(5)

except KeyboardInterrupt:
    client.loop_stop()
    client.disconnect()