import paho.mqtt.client as mqtt
import json
from environs import Env

env = Env()
env.read_env()

# variables required
mqtt_broker = env.str('mqtt-broker')

# Optional
mqtt_port = env.int('mqtt-port', 1883)
mqtt_username = env.str('mqtt-username', None)
mqtt_password = env.str('mqtt-password', None)
subscribe_topic = env.str('subscribe-topic', 'saveeye/telemetry')
publish_topic = env.str('publish-topic', 'saveeye/telemetry_calculated')


last_record = {}
last_values = []

def connect_mqtt():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    if mqtt_username or mqtt_password:
        client.username_pw_set(username=mqtt_username, password=mqtt_password)
    client.connect(mqtt_host, mqtt_port)
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
