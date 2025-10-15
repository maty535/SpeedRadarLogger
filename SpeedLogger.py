
#!/usr/bin/python3
import paho.mqtt.client as mqtt_client
import datetime as DT
import logging
import time
import json
from influxdb import InfluxDBClient
import random

client_id = f'weatherhub-mqtt-{random.randint(0, 100)}'
topic = "MobileAlerts/#"

logger = logging.getLogger('IOT.WeatherHub')

def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            logger.info("Connected to MQTT Broker!")
        else:
            logger.info("Failed to connect, return code %d\n", rc)

    broker = "192.168.3.4"
    logger.info("Trying to connect to mqtt")
    client = mqtt_client.Client(client_id)
    client.username_pw_set("iotImc", "M0jBr00k3r")
    client.on_connect = on_connect
    client.connect(broker)
    return client


def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        msg_decoded = msg.payload.decode()
        logger.info(f"Received `{msg_decoded}` from `{msg.topic}` topic")
        message_handler(client, msg_decoded, msg.topic)

    client.subscribe(topic)
    client.on_message = on_message


def message_handler(client, msg, topic):
    if topic.find("112568bfba42") > 0:
        handleTfaStation(topic, msg)
    elif topic.find("023049c7fb7b") > 0:
        handleOutdoorTemp(topic, msg)
    elif topic.find("0b12eff0ea17") > 0:
        handleWindSensor(topic, msg)
    elif topic.find("082972168812") > 0:
        handleRainSensor(topic, msg)


def handleTfaStation(topic, msg):
    data = dict()
    sensorData = json.loads(msg)
    # create data
    data["time"] = sensorData['t']

    logger.info("Storing data: " + topic)

    try:
        data["sensor"] = "Indoor-Prizemie"
        data["temp"] = 1.0*sensorData["temperature1"][0]
        data["hum"] = 1.0*sensorData["humidity1"][0]

        # send to influx only when data are valid
        if data["temp"] > -9999.0:
            sendToInflux(data)

        data["sensor"] = "Indoor-Poschodie"
        data["temp"] = 1.0*sensorData["temperatureIN"][0]
        data["hum"] = 1.0*sensorData["humidityIN"][0]
        if data["temp"] > -9999.0:
            sendToInflux(data)

        data["sensor"] = "Indoor-chyzka"
        data["temp"] = 1.0*sensorData["temperature2"][0]
        data["hum"] = 1.0*sensorData["humidity2"][0]
        if data["temp"] > -9999.0:
            sendToInflux(data)

        data["sensor"] = "Bikos outdoor"
        data["temp"] = 1.0*sensorData["temperature3"][0]
        data["hum"] = 1.0*sensorData["humidity3"][0]
        if data["temp"] > -9999.0:
            sendToInflux(data)

    except Exception as e:
        logger.error(e, 'Problem with influx db insert.')


def handleOutdoorTemp(topic, msg):
    data = dict()
    sensorData = json.loads(msg)
    # create data
    data["time"] = sensorData['t']

    logger.info("Storing data: " + topic)

    try:
        data["sensor"] = "Bikos outdoor.2"
        data["temp"] = 1.0*sensorData["temperature"][0]
        # send to influx only when data are valid
        if data["temp"] > -9999.0:
            sendTempToInflux(data)

    except Exception as e:
        logger.error(e, 'Problem with influx db insert.')


def handleWindSensor(topic, msg):
    data = dict()
    sensorData = json.loads(msg)
    # create data
    data["time"] = sensorData["t"]

    logger.info("Storing data: " + topic)

    try:
        data["sensor"] = "Bikos wind"
        data["directionDegree"] = 1.0*sensorData["directionDegree"]
        data["direction"] = sensorData["direction"]
        data["speed"] = 1.0*sensorData["windSpeed"]
        data["gustSpeed"] = 1.0*sensorData["gustSpeed"]
        data["lastTransmit"] = sensorData["lastTransmit"]

        # send to influx only when data are valid
        sendWindDataToInflux(data)

    except Exception as e:
        logger.error(e, 'Problem with influx db insert.')


def sendWindDataToInflux(mData):

    json_body = [
        {

            "measurement": "wind",
            "time": mData["time"],

            "tags": {
                'location': mData["sensor"]
            },
            "fields": {
                "speed":   		mData["speed"],
                "gustSpeed": 	mData["gustSpeed"],
                "directionDegree":	mData["directionDegree"],
                "direction":	mData["direction"],
                "lastTransmit":	mData["lastTransmit"]
            }
        }
    ]

    influxdb_client.write_points(json_body)
    logger.info('Wind data stored to influx DB')


def handleRainSensor(topic, msg):
    data = dict()
    sensorData = json.loads(msg)
    # create data
    logger.info("Storing data: " + topic)

    try:
        if sensorData["eventTimes"][0] == 0:
            # impuls has been generated - bucket filled
            data["sensor"] = "Bikos rainfall"
            data["value"] = 0.258*sensorData["eventCounter"]
            data["flipCount"] = sensorData["eventCounter"]
            data["time"] = sensorData["t"]
            data["eventDuration"] = sensorData["eventTimes"][1]

            # send to influx only when data are valid and value has changed
            # if( prev count < actual count)
            sendRainfallToInflux(data)

    except Exception as e:
        logger.error(e, 'Problem with influx db insert.')


def sendRainfallToInflux(mData):
    json_body = [
        {

            "measurement": "rain",
            "time": mData["time"],

            "tags": {
                'location': mData["sensor"]
            },
            "fields": {
                "flipCount":        mData["flipCount"],
                "value":        	mData["value"],
                "eventDuration":	mData["eventDuration"]
            }
        }
    ]

    influxdb_client.write_points(json_body)
    logger.info('Rain data stored to influx DB')


def sendTempToInflux(mData):

    json_body = [
        {
            "measurement": "temperature",
            "time": mData["time"],

            "tags": {
                'location': mData["sensor"]
            },
            "fields": {
                "val":  mData["temp"]
            }
        }
    ]

    influxdb_client.write_points(json_body)
    logger.info('Temperature data stored to influx DB')


def sendToInflux(mData):

    json_body = [
        {
            "measurement": "humidity",
            "time": mData["time"],

            "tags": {
                'location': mData["sensor"]
            },
            "fields": {
                "val":  mData["hum"]
            }
        },
        {
            "measurement": "temperature",
            "time": mData["time"],

            "tags": {
                'location': mData["sensor"]
            },
            "fields": {
                "val":  mData["temp"]
            }
        }
    ]

    influxdb_client.write_points(json_body)
    logger.info('Weather Station data stored to influx DB')


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

    # Initialize influx conection
    influxdb_client = InfluxDBClient('192.168.3.4', 8086)
    influxdb_client.switch_database('home_data')
    logger.info('Connected to Influx DB')

    run()
