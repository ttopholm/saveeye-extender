from paho.mqtt import client as mqtt_client
import random
import time
import json
import math
import statistics
import os


mqtt_host = os.environ.get('mqtt-host', '127.0.0.1')
port = os.environ.get('mqtt-port', 1883)
subscribe_topic = os.environ.get('subscribe-topic', 'saveeye/telemetry')
publish_topic = os.environ.get('publish-topic', 'saveeye/telemetry_calculated')
client_id = f'python-mqtt-{random.randint(0, 1000)}'
last_record = {}
last_values = []
factor = os.environ.get('factor', 0.4)
max_data_points = os.environ.get('max-data-points', 30)

def connect_mqtt():
    client = mqtt_client.Client(client_id)
    client.connect(mqtt_host, port)
    return client


def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        global last_record, last_values
        msg_json = json.loads(msg.payload.decode())
        if 'extenderPulses' not in last_record:
            last_record = msg_json
        elif 'extenderPulses' in last_record and 'extenderPulses' in msg_json  and msg_json.get('extenderAdvType') == 1 and msg_json.get('extenderPulses') > last_record.get('extenderPulses'):
            pulses = msg_json.get('extenderPulses') - last_record.get('extenderPulses')
            time_between_pulses = (msg_json.get('extenderTimestamp') - last_record.get('extenderTimestamp'))
            currentpower  = 3600000000 / ((time_between_pulses / pulses))/ 1000
            client.publish(topic,
                json.dumps({'currentConsumptionWatt': currentpower})
            )

            last_record  = msg_json
    client.subscribe(subscribe_topic)
    client.on_message = on_message



def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()


if __name__ == '__main__':
    run()
