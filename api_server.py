from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from influxdb import InfluxDBClient
import time
import os
from datetime import datetime, timezone

app = Flask(__name__)
CORS(app)
client_db = InfluxDBClient(host="localhost", port=8086, database="iot_data")

FIRMWARE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'firmware')
LATEST_FIRMWARE_VERSION = "v2.0.0"
LATEST_FIRMWARE_FILENAME = "firmware_v2.bin"

@app.route('/api/telemetry', methods=['GET'])
def get_telemetry():
    try:
        # Menarik 3 parameter baru dari InfluxDB
        res_dust = client_db.query("SELECT * FROM sensor_reading WHERE sensor='sharp_dust' ORDER BY time DESC LIMIT 15")
        res_temp = client_db.query("SELECT * FROM sensor_reading WHERE sensor='dht_temp' ORDER BY time DESC LIMIT 15")
        res_hum = client_db.query("SELECT * FROM sensor_reading WHERE sensor='dht_hum' ORDER BY time DESC LIMIT 15")
        
        return jsonify({
            "status": "success", 
            "data": {
                "dust": list(res_dust.get_points()),
                "temperature": list(res_temp.get_points()),
                "humidity": list(res_hum.get_points())
            }
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/status', methods=['GET'])
def get_status():
    try:
        res = client_db.query("SELECT * FROM actuator_status ORDER BY time DESC LIMIT 1")
        points = list(res.get_points())
        
        if points:
            latest = points[0]
            db_time_str = latest['time'] 
            db_time_obj = datetime.strptime(db_time_str[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
            latest['age_s'] = int(time.time() - db_time_obj.timestamp())
        else:
            latest = {"status_text": "UNKNOWN", "status_value": 0, "age_s": -1}
            
        return jsonify({"status": "success", "data": latest})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/ota/check', methods=['GET'])
def check_ota():
    current_ver = request.args.get('current_ver')
    if current_ver == LATEST_FIRMWARE_VERSION:
        return jsonify({"status": "success", "update_available": False, "message": "Firmware up-to-date"})
    return jsonify({
        "status": "success", 
        "update_available": True, 
        "latest_version": LATEST_FIRMWARE_VERSION,
        "download_url": f"/api/ota/download/{LATEST_FIRMWARE_FILENAME}"
    })

@app.route('/api/ota/download/<filename>', methods=['GET'])
def download_ota(filename):
    try:
        return send_from_directory(FIRMWARE_DIR, filename, as_attachment=True)
    except FileNotFoundError:
        return jsonify({"status": "error", "message": "File firmware tidak ditemukan"}), 404

if __name__ == '__main__':
    print("[API SERVER] Menginisiasi REST API Lab SIS di http://127.0.0.1:5000")
    if not os.path.exists(FIRMWARE_DIR): os.makedirs(FIRMWARE_DIR)
    app.run(host='127.0.0.1', port=5000, debug=False)