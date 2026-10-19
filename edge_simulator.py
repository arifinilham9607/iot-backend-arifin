import json
import time
import random
import threading
import paho.mqtt.client as mqtt

BROKER = "localhost"
PORT = 1883
DATA_TOPIC = "dt/arifin-node-01/telemetry"
CMD_TOPIC = "cmd/arifin-node-01/action"

# State Perangkat Keras Internal
actual_pump_state = "OFF"
last_cmd_received_time = time.time()
EDGE_FAILSAFE_TIMEOUT = 20 # Detik sebelum perangkat mematikan pompa sendiri

def edge_failsafe_loop():
    global actual_pump_state
    while True:
        time.sleep(2)
        # Poin 1: Failsafe Sisi Perangkat
        if time.time() - last_cmd_received_time > EDGE_FAILSAFE_TIMEOUT:
            if actual_pump_state != "SAFE_OFF":
                print(f"[HW-FAILSAFE] Koneksi/Perintah terputus > {EDGE_FAILSAFE_TIMEOUT}s. Memaksa relai pompa MATI (SAFE_OFF).")
                actual_pump_state = "SAFE_OFF"

def on_connect(client, userdata, flags, rc):
    print(f"[EDGE] Terhubung ke Broker. Kode status: {rc}")
    client.subscribe(CMD_TOPIC, qos=1) # Menggunakan QoS 1

def on_message(client, userdata, msg):
    global actual_pump_state, last_cmd_received_time
    try:
        payload = json.loads(msg.payload.decode())
        command = payload.get("command")
        cmd_id = payload.get("cmd_id", "unknown")
        
        last_cmd_received_time = time.time() # Reset timer failsafe perangkat
        
        if command == "PUMP_ON":
            actual_pump_state = "ON"
        elif command in ["PUMP_OFF", "PUMP_DEFAULT"]:
            actual_pump_state = "OFF"
            
        print(f"[AKTUATOR] Mengeksekusi: {actual_pump_state} | Cmd ID: {cmd_id}")
    except Exception as e:
        print(f"[EDGE] Payload perintah cacat: {e}")

client = mqtt.Client(client_id="edge_sim_01")
client.on_connect = on_connect
client.on_message = on_message
client.connect(BROKER, PORT, 60)
client.loop_start()

# Jalankan watchdog perangkat keras di latar belakang
threading.Thread(target=edge_failsafe_loop, daemon=True).start()

seq_num = 1
print("[EDGE] Memulai siklus telemetri loop tertutup...")

try:
    while True:
        temp = round(random.uniform(29.0, 32.0), 2)
        co2 = round(random.uniform(400.0, 800.0), 2)
        
        # Simulasi sensor rusak sesekali (Poin 2)
        sensor_quality = "ok" if random.random() > 0.05 else "error"
        
        payload = {
            "schema_ver": "1.1",
            "device_id": "arifin-node-01",
            "seq_num": seq_num,
            "timestamp_ms": int(time.time() * 1000),
            "data": [
                {"sensor": "sensor_A", "value": temp, "unit": "celcius", "quality": sensor_quality},
                {"sensor": "sensor_B", "value": co2, "unit": "ppm", "quality": "ok"}
            ],
            "sys": {
                "fw_ver": "v0.1.1-sim",
                "uptime_s": seq_num * 5,
                "actuator_state": actual_pump_state # Poin 4: Pelaporan loop tertutup
            }
        }
        
        client.publish(DATA_TOPIC, json.dumps(payload), qos=1)
        print(f"[TX] Seq {seq_num} | Suhu: {temp}C ({sensor_quality}) | Relai: {actual_pump_state}")
        
        seq_num += 1
        time.sleep(5)
except KeyboardInterrupt:
    client.loop_stop()
    client.disconnect()