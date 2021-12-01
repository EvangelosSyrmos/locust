from locust import User
from tasker import TaskerManager
# from httpmanager import HttpManager
import paho.mqtt.client as mqtt


NUMBER_OF_CLIENTS = 0
# broker = 'localhost'
broker = 'testingmqtt.cloud.shiftr.io'
username = 'testingmqtt'
password = 'j7xJn5DDE9f1azat'
port = 1883


def incease_client_id():
    global NUMBER_OF_CLIENTS
    NUMBER_OF_CLIENTS += 1

class MQTTLocust(User):
    '''
    This class instantiates an MQTT client and 
    '''
    tasks = {TaskerManager}
    # tasks = {TaskerManager:1, HttpManager:1}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        incease_client_id()
        self.client_id = f'Device_ID-{NUMBER_OF_CLIENTS}'
        self.client = mqtt.Client(self.client_id)
        self.client.username_pw_set(username, password)
        self.client.on_connect = self.on_connect

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print(f'Connected to MQTT Broker: {self.client_id}')
        else:
            print(f'Failed to connect {self.client_id}, status: {rc}')

