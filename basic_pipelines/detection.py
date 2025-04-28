import gi
import json
import os
import numpy as np
import cv2
import hailo
import paho.mqtt.client as mqtt  

gi.require_version('Gst', '1.0')
from gi.repository import Gst, GLib

from hailo_apps_infra.hailo_rpi_common import (
    get_caps_from_pad,
    get_numpy_from_buffer,
    app_callback_class,
)
from hailo_apps_infra.detection_pipeline import GStreamerDetectionApp

# Configuración del broker MQTT de ThingsBoard
THINGSBOARD_BROKER = "" #Dirección del brocker  
THINGSBOARD_PORT = 1883 
THINGSBOARD_ACCESS_TOKEN = "" #Colocar Token proporcionado por Thingsboard 
MQTT_TOPIC = "v1/devices/me/telemetry"  

mqtt_client = mqtt.Client()
mqtt_client.username_pw_set(THINGSBOARD_ACCESS_TOKEN) 

last_person_count = None

def ensure_mqtt_connection():
    try:
        if not mqtt_client.is_connected():
            print("Reconectando a ThingsBoard...")
            mqtt_client.connect(THINGSBOARD_BROKER, THINGSBOARD_PORT, 60)
    except Exception as e:
        print(f"No se pudo conectar a ThingsBoard: {e}")

def send_to_thingsboard(data):
    try:
        payload = json.dumps(data)
        mqtt_client.publish(MQTT_TOPIC, payload)
        print(f"Datos enviados a ThingsBoard: {payload}")
    except Exception as e:
        print(f"Error al enviar datos a ThingsBoard: {e}")

class user_app_callback_class(app_callback_class):
    def __init__(self):
        super().__init__()

def app_callback(pad, info, user_data):
    global last_person_count, last_car_count

    buffer = info.get_buffer()
    if buffer is None:
        return Gst.PadProbeReturn.OK

    user_data.increment()

    format, width, height = get_caps_from_pad(pad)
    frame = None
    if user_data.use_frame and format is not None and width is not None and height is not None:
        frame = get_numpy_from_buffer(buffer, format, width, height)

    roi = hailo.get_roi_from_buffer(buffer)
    detections = roi.get_objects_typed(hailo.HAILO_DETECTION)

    person_count = 0

    for detection in detections:
        label = detection.get_label()
        if label == "person":
            person_count += 1

    if person_count != last_person_count:
        payload = {
            "Personas": person_count
        }
        send_to_thingsboard(payload)
        last_person_count = person_count

    if user_data.use_frame:
        cv2.putText(frame, f"Personas: {person_count}", (10, 30), cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 255, 0), 2)
        frame = cv2.cvtColor(frame, cv2.COLOR_RGB2BGR)
        user_data.set_frame(frame)

    return Gst.PadProbeReturn.OK

if __name__ == "__main__":
    try:
        mqtt_client.connect(THINGSBOARD_BROKER, THINGSBOARD_PORT, 60)  
        user_data = user_app_callback_class()
        app = GStreamerDetectionApp(app_callback, user_data)
        app.run()
    except Exception as e:
        print(f"Error al inicializar: {e}")
