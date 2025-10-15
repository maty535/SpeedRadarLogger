
#!/usr/bin/python3
import paho.mqtt.client as mqtt
import datetime as DT
import logging
import time
import json
import os
from influxdb import InfluxDBClient
import random
from dotenv import load_dotenv
import ssl

client_id = f'speedLogger-mqtt-{random.randint(0, 100)}'
#client_id = "mqtt-explorer-ivanecky"
topic = "dvcs/1/mprsv/traffic/sensors/speed/DDATA/sprava-komunikacii/roadsense/radarix/radar-1/default/device/#"

logger = logging.getLogger('Presov.SpeedLogger')

def connect_mqtt() -> mqtt:
    def on_connect(client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            logger.info("Connected to MQTT Broker!")
        else:
            logger.info("Failed to connect, return code %d\n", reason_code)


    #Pripojenie na špeciálny port
    try:
        logger.info("Trying to connect to mqtt")
        client = mqtt.Client(client_id=client_id, 
                             protocol=mqtt.MQTTv311, 
                             callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        client.username_pw_set(os.getenv("MQTT_UID"), os.getenv("MQTT_PWD"))
        client.on_connect = on_connect
        # NOVÁ A ZJEDNODUŠENÁ KONFIGURÁCIA TLS:
        client.tls_set(
            ca_certs=None,            # Zmenené na None – skúsi použiť systémové CA
            certfile=None,            # Zmenené na None – nepoužívame klientsky certifikát
            keyfile=None,             # Zmenené na None – nepoužívame klientsky kľúč
            cert_reqs=ssl.CERT_REQUIRED, # Stále vyžadujeme overenie servera (odporúčané)
            tls_version=ssl.PROTOCOL_TLS
        )
        #client.tls_insecure_set(True)
        client.connect(os.getenv("BROKER_ADDRESS"), int(os.getenv("BROKER_PORT")))
    except Exception as e:
        print(f"Nastala chyba počas pripojenia: {e}")
        exit()
    
    logger.info(f"Connected to mqtt server:{os.getenv("BROKER_ADDRESS")}")
    
    return client


def subscribe(client: mqtt):
    def on_message(client, userdata, msg):
        msg_decoded = msg.payload.decode()
        logger.info(f"Received `{msg_decoded}` from `{msg.topic}` topic")
        message_handler(client, msg_decoded, msg.topic)

    client.subscribe(topic)
    client.on_message = on_message


def message_handler(client, msg, topic):
    handleSpeedSensor(topic, msg)

def sendSpeedToInflux(data):
    json_body = [
        {

            "measurement": "speed",
            "time": data["time"],

            "tags": {
                'location': data["sensor"]
            },
            "fields": {
                "speed":   		data["speed"],
                "limit_exceeded": 	data["limit_exceeded"],
                "overspeed":	data["overspeed"]
            }
        }
    ]

    influxdb_client.write_points(json_body)
    logger.info('Speed data stored to influx DB')


def handleSpeedSensor(topic, msg):
    data = dict()
    sensorData = json.loads(msg)
    logger.info("Storing data: " + topic)
    
    # create data
   

    try:
        data["sensor"]  = "SpeedSensor.Solivar"
        data["time"]    = sensorData['metrics']['vehicle_measurement/speed_measurement_time']['value']
        data["speed"]   = sensorData['metrics']["vehicle_measurement/speed"]['value']
        data["limit_exceeded"]  = sensorData['metrics']["vehicle_measurement/limit_exceeded"]['value']
        data["overspeed"]       = sensorData['metrics']["vehicle_measurement/speed"]['value'] - 50
        
        
        logger.info(f"Storing data: {data}")
        
        # send to influx only when data are valid
        if data["speed"] > 0:
            sendSpeedToInflux(data)

    except Exception as e:
        logger.error(e, 'Problem with influx db insert.')

def has_changed(client, topic, msg):
    topic2 = topic.lower()
    if topic2.find("control") != -1:
        return False
    if topic in client.last_message:
        if client.last_message[topic] == msg:
            return False
    client.last_message[topic] = msg
    return True


def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()


if __name__ == '__main__':

    FORMAT = '%(levelname)s - %(module)s.%(funcName)s: %(message)s'
    logging.basicConfig(format=FORMAT)

    logger.setLevel(logging.DEBUG)
    logger.info('Application run at: %s', DT.datetime.today())

    load_dotenv()
    # Initialize influx conection
    influxdb_client = InfluxDBClient('192.168.3.4', 8086)
    influxdb_client.switch_database('presov_public')
    logger.info('Connected to Influx DB')

    run()
