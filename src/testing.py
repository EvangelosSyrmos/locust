from os import terminal_size
from locust import User
from locust.env import Environment
from datetime import datetime, timedelta
from uuid import uuid4
from enum import Enum
import time
import json
import random
import ssl
import paho.mqtt.client as mqtt

with open('settings.json') as f:
    SETTINGS = json.load(f)

def time_delta(t1, t2):
    return int((t2 - t1) * 1000)

class ConnectError(Exception):
    pass

class ValueError(Exception):
    pass

class TimeoutError(Exception):
    pass

class MsgType(Enum):
    PUB = 'PUB'
    SUB = 'SUB'

class EventType(Enum):
    CONNECT = 'connect'
    DISCONNECT ='disconnect'
    PUBLISH = 'publish'
    SUBSCRIBE = 'subscribe'
    TLS_SET = 'tls_set'

class Message:
    
    def __init__(self, type, qos, topic, start_time, timeout, name, payload=None):
        self.type = type,
        self.qos = qos,
        self.topic = topic
        self.payload = payload
        self.start_time = start_time
        self.timeout = timeout
        self.name = name

    def timed_out(self, total_time):
        return self.timeout is not None and total_time > self.timeout

class MQTTClient(mqtt.Client):

    def __init__(self, environment, *args, **kwargs):
        super().__init__(MQTTClient, self).__init__(*args, **kwargs)
        self.environment = environment
        self.on_publish = self._on_publish_cb
        self.on_subscribe = self._on_subscribe_cb
        self.on_disconnect = self._on_disconnect_cb
        self.on_connect = self._on_connect_cb
        self.pub_message_map = {}
        self.sub_message_map = {}
        self.default_qos = 0
    
    def tls_set(self, 
                ca_certs, 
                certfile=None, 
                keyfile=None,
                cert_reqs=ssl.CERT_REQUIRED,
                tls_version=ssl.PROTOCOL_TLSv1,
                ciphers=None):
        start_time = time.time()
        try:
            super(MQTTClient, self).tls_set(ca_certs,
                                            certfile,
                                            keyfile,
                                            cert_reqs,
                                            tls_version,
                                            ciphers)
        except Exception as e:
            self.environment.events.request.fire(
                request_type=SETTINGS["REQUEST_TYPE"],
                name=EventType.TLS_SET,
                response_time=time_delta(start_time, time.time()),
                exception=e
            )

    def publish(self, topic, payload=None, qos=0, **kwargs):
        timeout = kwargs.pop('timeout', 10000)
        start_time = time.time()

        try:
            err, mid = super(MQTTClient, self).publish(
                topic,
                payload=payload,
                qos=qos,
                **kwargs
            )
            if err:
                self.environment.events.request.fire(
                    request_type=SETTINGS["REQUEST_TYPE"],
                    name=EventType.PUBLISH,
                    response_time=time_delta(start_time, time.time()),
                    response_length=0,
                    exception=ValueError(err)
                )
            self.pub_message_map[mid] = Message(
                MsgType.PUB,
                qos,
                topic,
                payload,
                start_time,
                timeout,
                name=EventType.PUBLISH
            )
        except Exception as e:
            self.environment.events.request.fire(
                    request_type=SETTINGS["REQUEST_TYPE"],
                    name=EventType.PUBLISH,
                    response_time=time_delta(start_time, time.time()),
                    response_length=0,
                    exception=e)
    
    def subscribe(self, topic, qos=0):
        timeout = 15000
        start_time = time.time()
        try:
            err, mid = super(MQTTClient, self).subscribe(
                topic,
                qos=qos
            )
            self.sub_message_map[mid] = Message(
                MsgType.SUB,
                qos,
                topic,
                start_time,
                timeout,
                name=EventType.SUBSCRIBE
            )
            if err:
                raise ValueError(err)
        except Exception as e:
            self.environment.events.request.fire(
               request_type=SETTINGS["REQUEST_TYPE"],
                name=EventType.SUBSCRIBE,
                response_time=time_delta(start_time, time.time()),
                response_length=0,
                exception=e)
    
    def _on_connect_cb(self, client, flags_dict, userdata, rc):
        if rc != 0:
            self.environment.events.request.fire(
                request_type=SETTINGS["REQUEST_TYPE"],
                name=EventType.CONNECT,
                response_time=0,
                response_length=0,
                exception=rc
            )
        else:
            self.environment.events.request.fire(
                request_type=SETTINGS["REQUEST_TYPE"],
                name=EventType.CONNECT,
                response_time=0,
                response_length=0,
                exception=None
            )
    
    def _on_publish_cb(self, client, userdata, mid):
        end_time = time.time()
        if self.default_qos == 0:
            time.sleep(float(0.5))
        
        message = self.pub_message_map.pop(mid, None)

        if message is None:
            self.environment.events.request.fire(
                request_type=SETTINGS["REQUEST_TYPE"],
                name=EventType.CONNECT,
                response_time=0,
                response_length=0,
                exception=ValueError("Published message can not be found")
            )
            return
        
        total_time = time_delta(message.start_time, end_time)
        if message.timed_out(total_time):
            self.environment.events.request.fire(
                request_type=SETTINGS["REQUEST_TYPE"],
                name=message.name,
                response_time=total_time,
                response_length=0,
                exception=TimeoutError("Publish timed out")
            )
        else:
            self.environment.events.request.fire(
                request_type=SETTINGS["REQUEST_TYPE"],
                name=message.name,
                response_time=total_time,
                response_length=len(message.payload),
                exception=None
            )

    def _on_subscribe_cb(self, client, userdata, mid, granted_qos):
        end_time = time.time()
        message = self.sub_message_map.pop(mid, None)
        if message is None:
            print("Message not found for on_subscribe")
            return
        total_time = time_delta(message.start_time, end_time)
        if message.timed_out(total_time):
            self.environment.events.request.fire(
                request_type=SETTINGS["REQUEST_TYPE"],
                name=message.name,
                response_time=total_time,
                response_length=0,
                exception=TimeoutError("Subscribe timed out")
            )
        else:
            self.environment.events.request.fire(
                request_type=SETTINGS["REQUEST_TYPE"],
                name=message.name,
                response_time=total_time,
                response_length=0,
                exception=None
            )
            print("Subsrcibe success!")
    
    def _on_disconnect_cb(self, client, userdata, rc):
        if rc != 0:
            self.environment.events.request.fire(
                    request_type=SETTINGS["REQUEST_TYPE"],
                    name=EventType.DISCONNECT,
                    response_time=0,
                    response_length=0,
                    exception=rc
                )
        else:
            self.environment.events.request.fire(
                request_type=SETTINGS["REQUEST_TYPE"],
                name=EventType.DISCONNECT,
                response_time=0,
                response_length=0,
                exception=None
            )
            self.reconnect()



class MQTTUser(User):
    abstract = True
    
    def __init__(self, environment, *args, **kwargs):
        super().__init__(environment)
        self.client_id = uuid4()
        # Create an Mqtt client 
        self.client = MQTTClient(client_id=self.client_id,
            protocol=mqtt.MQTTv311,
            environment=self.environment)
        try:
            self.client.username_pw_set(
                username=SETTINGS["USERNAME"],
                password=SETTINGS["PASSWORD"])
            
            self.client.tls_set(ca_certs=SETTINGS["CAFILE"], 
                                certfile=SETTINGS["CERTFILE"], 
                                keyfile=SETTINGS["KEYFILE"],
                                tls_version=ssl.PROTOCOL_TLSv1)
            
            self.client.connect_async(SETTINGS["BROKER"], SETTINGS["PORT"])
            self.client.loop_start()
        except Exception as e:
            self.environment.events.request.fire(
                request_type=SETTINGS["REQUEST_TYPE"],
                name=EventType.CONNECT,
                response_time=time_delta(start_time, time.time()),
                response_length=0, 
                exception=ConnectError("Could not connect to host")
            )