from os import name
import time 
import ssl
import json
import paho.mqtt.client as mqtt
from uuid import uuid1
from typing import Any, Optional, List
from locust import events, User
from locust.env import Environment
from enum import Enum
from dataclasses import dataclass

SUBACK_FAILURE = 0x80

class EventType(Enum):
    CONNECT: 'connect'
    DISCONNECT: 'disconnect'
    PUBLISH: 'publish'
    SUBSCRIBE: 'subscribe'


class RequestType(Enum):
    MQTT: 'MQTT'

@dataclass
class Message:
    """
    This class is responsible for storing the messages
    """
    topic: str
    start_time: float
    payload: Optional[Any]=None
    qos: int = 0

class MqttUser(User):
    abstract = True

    with open('settings.json') as f:
        SETTINGS = json.load(f)

    username = SETTINGS['USERNAME']
    password = SETTINGS['PASSWORD']
    _cafile = SETTINGS['CAFILE']
    transport = SETTINGS['TRANSPORT']
    # tls_context = SETTINGS['TLS_CONTEXT']
    broker = SETTINGS['BROKER']
    port = SETTINGS['PORT']

    def __init__(self, environment: Environment):
        super().__init__(environment)
        self.client = MqttClient(
            broker=self.broker,
            port=self.port,
            username=self.username,
            password=self.password,
            environment=self.environment,
            client_id=self.client_id,
            transport=self.transport
        )

        if self.port == 8883:
            assert self._cafile is not None, "CA-File required"

        if self.port == 8883:
            self.client.tls_set(ca_certs=self._cafile)
            self.client.username_pw_set(
                self.username,
                self.password
            )
            self.client.connect(host=self.broker, port=self.port)
        elif self.port == 1883:
            self.client.connect(host=self.broker, port=self.port)
        
        self.client.loop_start()


class MqttClient(mqtt.Client):
    def __init__(
        self,
        *args,
        broker,
        port,
        username,
        password,
        environment: Environment,
        transport,
        client_id: Optional[str],
        cafile: Optional[Any] = None,
        certfile: Optional[Any] = None,
        topics: Optional[List[str]] = None,
        **kwargs
    ):

        # Check for client ID, if none create a UUID random
        if not client_id:
            self.client_id = f"locust-{uuid1()}"
        else:
            self.client_id = client_id

        super().__init__(
            environment=
            client_id=self.client_id,
            clean_session=True,
            userdata=None,
            transport=self.transport,
            **kwargs
        )
        self.username = username
        self.password = password
        self._cafile = cafile
        self.topics = topics
        self.broker = broker

        # self.client = mqtt.Client(self.client_id)

        # Connect with password
        # if self.port == 8883:
        #     self.client.tls_set(ca_certs=self._cafile)
        #     self.client.username_pw_set(
        #         self.username,
        #         self.password
        #     )
        #     self.client.connect(
        #         host=self.broker, 
        #         port=self.port)
        # elif self.port == 1883:
        #     self.client.connect(
        #         host=self.broker, 
        #         port=self.port)
        
        # Set callback functions
        self.environment = environment
        self.on_connect = self.on_connect
        self.on_disconnect = self.on_disconnect
        self.on_publish = self.on_publish
        self.on_subscribe = self.on_subscribe

        self.client.published_messages = {}
        self.client.subscribed_messages = {}
    
    def get_total_duration(self, t_start, t_end):
        """
        Calculate the duration of the given task
        """
        return int((t_end-t_start)*1000)

    def on_connect(self, client, userdata, flags, rc):
        if rc != 0:
            self.environment.events.request.fire(
                request_type=RequestType.MQTT,
                name=EventType.CONNECT,
                response_time=0,
                response_length=0,
                exception=rc,
                context={
                    "client_id": self.client_id,
                },
            )
        else:
            self.environment.events.request.fire(
                request_type=RequestType.MQTT,
                name=EventType.CONNECT,
                response_time=0,
                response_length=0,
                exception=None,
                context={
                    "client_id": self.client_id,
                },
            )
    
    def on_disconnect(self, client, userdata, rc):
        if rc != 0:
            self.environment.events.request.fire(
                request_type=RequestType.MQTT,
                name=EventType.DISCONNECT,
                response_time=0,
                response_length=0,
                exception=rc,
                context={
                    "client_id": self.client_id,
                },
            )
        else:
            self.environment.events.request.fire(
                request_type=RequestType.MQTT,
                name=EventType.Di,
                response_time=0,
                response_length=0,
                exception=None,
                context={
                    "client_id": self.client_id,
                },
            )

    def on_publish(self, client, userdata, mid):
        end_time = time.time()
        try:
            message = client.published_messages.pop(mid, None)
        except KeyError:
            self.environment.events.request.fire(
                request=RequestType.MQTT,
                name=EventType.PUBLISH,
                response_time=0,
                response_length=0,
                exception=AssertionError(f"Could not find message data for"
                " mid {mid} in published_messages"),
                context={
                    "client_id": self.client_id,
                    "mid": mid,
                },
            )
        else:
            # This will run in case there is no Exception
            self.environment.events.request.fire(
               request=RequestType.MQTT,
                name=EventType.PUBLISH,
                response_time=self.get_total_duration(
                    message.start_time,
                    end_time),
                response_length=len(message.payload),
                exception=None,
                context={
                    "client_id": self.client_id,
                    **message._asdict(),
                }, 
            )
    
    def on_subscribe(self, client, userdata, mid, qos):
        end_time = time.time()
        try:
            message = client.subscribed_messages.pop(mid, None)
        except KeyError:
            self.environment.events.request.fire(
                request=RequestType.MQTT,
                name=EventType.SUBSCRIBE,
                response_time=0,
                response_length=0,
                exception=AssertionError(f"Could not find message data for"
                " mid {mid} in subscribed_messages"),
                context={
                    "client_id": self.client_id,
                    "mid": mid,
                },
            )
        else:
            """
            http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Figure_3.26_-
            """
            if 0x80 in qos:
                # If SUBACK Failed it will return 0x80
                self.environment.events.request.fire(
                    request=RequestType.MQTT,
                    name=EventType.SUBSCRIBE,
                    response_time=self.get_total_duration(
                        message.start_time,
                        end_time),
                    response_length=0,
                    exception=AssertionError(f"Broker returned error during"
                    " subscription: {qos}"),
                    context={
                        "client_id": self.client_id,
                        **message._asdict(),
                    },
                )
            else:
                # This will run if subrciption worked
                self.environment.events.request.fire(
                    request=RequestType.MQTT,
                    name=EventType.SUBSCRIBE,
                    response_time=self.get_total_duration(
                        message.start_time,
                        end_time),
                    response_length=0,
                    exception=None,
                    context={
                        "client_id": self.client_id,
                        **message._asdict(),
                    },
                )
