from __future__ import annotations

import random
import time
import typing
import json

from datetime import datetime

from locust import User, task, TaskSet, events
from locust.env import Environment
from locust.user.wait_time import between, constant_throughput

from uuid import uuid4
from typing import Any, Optional, List
from enum import Enum
from dataclasses import dataclass

import paho.mqtt.client as mqtt

if typing.TYPE_CHECKING:
    from paho.mqtt.properties import Properties
    from paho.mqtt.subscribeoptions import SubscribeOptions


with open('settings.json') as f:
        SETTINGS = json.load(f)

with open('200_bytes.json') as f:
    PAYLOAD_200_BYTES = json.dumps(json.load(f))
with open('500_bytes.json') as f:
    PAYLOAD_500_BYTES = json.dumps(json.load(f))
with open('700_bytes.json') as f:
    PAYLOAD_700_BYTES = json.dumps(json.load(f))

# with open('payload.json') as f:
#     PAYLOAD_DATA = json.dumps(json.load(f))

class OnStart_Connect_Sub_Task_PuB_At_Time_Intervals_Clients(TaskSet):
    '''
    This class creates clients that:
    --------------------------------
    ==> On connect:
        => Connects to the broker once
        => Subscribes to all topics once
    ==> During tasks:
        => Publish to a random topic at a high frequency
    '''
    initial_publish = True
    # wait_time = constant_throughput(
    #     SETTINGS['LOCUST_VARIABLES']['REQUESTS_PER_SEC_HIGH_FREQ'])

    @events.test_start.add_listener
    def on_test_start(environment, **kwargs):
        print("Starting test")
    
    def on_start(self):
        for topic in SETTINGS['TOPICS']['TEST_TOPICS']:
            self.client.subscribe(topic, qos=0)
    
    @task
    def high_frequency_publish_task(self):
        if not self.initial_publish:
            if int((datetime.now() - self.last_published_timestamp_of_two_seconds).seconds) >= SETTINGS["PUBLISH_INTERVALS"]["2"]:
                self.client.publish(random.choice(SETTINGS["TOPICS"]['TEST_TOPICS']),
                                    payload = PAYLOAD_200_BYTES)
                self.last_published_timestamp_of_two_seconds = datetime.now()
                print(f"{self.client.client_id} => P: {SETTINGS['PUBLISH_INTERVALS']['2']} ==> {datetime.now()}")
            if int((datetime.now() - self.last_published_timestamp_of_ten_seconds).seconds) >= SETTINGS["PUBLISH_INTERVALS"]["10"]:
                self.client.publish(random.choice(SETTINGS["TOPICS"]['TEST_TOPICS']),
                                    payload = PAYLOAD_500_BYTES)
                self.last_published_timestamp_of_ten_seconds = datetime.now()
                print(f"{self.client.client_id} => P: {SETTINGS['PUBLISH_INTERVALS']['10']} ==> {datetime.now()}")
            if int((datetime.now() - self.last_published_timestamp_of_sixty_seconds).seconds) >= SETTINGS["PUBLISH_INTERVALS"]["60"]:
                self.client.publish(random.choice(SETTINGS["TOPICS"]['TEST_TOPICS']),
                                    payload = PAYLOAD_700_BYTES)
                self.last_published_timestamp_of_sixty_seconds = datetime.now()
                print(f"{self.client.client_id} => P: {SETTINGS['PUBLISH_INTERVALS']['60']} ==> {datetime.now()}")
        else:
            self.client.publish(random.choice(SETTINGS["TOPICS"]['TEST_TOPICS']),
                                payload = round(random.uniform(0, 1), 2))
            self.last_published_timestamp_of_two_seconds = self.last_published_timestamp_of_ten_seconds = self.last_published_timestamp_of_sixty_seconds = datetime.now()
            self.initial_publish = False
            print(f"{self.client.client_id} => First publish completed")

SUBACK_FAILURE = 0x80
CLIENT_COUNTER = 0

def increase():
    global CLIENT_COUNTER
    CLIENT_COUNTER += 1

class EventType(Enum):
    CONNECT = 'connect'
    DISCONNECT ='disconnect'
    PUBLISH = 'publish'
    SUBSCRIBE = 'subscribe'


class RequestType(Enum):
    MQTT = 'MQTT'


def _generate_mqtt_event_name(id, event_type: str, qos: int, topic: str):
    return f"{id}:{event_type}:{qos}:{topic}"


class PublishedMessageContext(typing.NamedTuple):
    qos: int
    topic: str
    start_time: float
    payload_size: int


class SubscribeContext(typing.NamedTuple):
    qos: int
    topic: str
    start_time: float

class MqttUser(User):
    # tasks = {OnStart_Connect_Sub_Task_PuB_High_Frequency_Clients: 4,
    #         OnStart_Connect_Sub_Task_PuB_Low_Frequency_Clients: 1}
    tasks = {OnStart_Connect_Sub_Task_PuB_At_Time_Intervals_Clients}
    
    username = SETTINGS['CREDENTIALS']['USERNAME']
    password = SETTINGS['CREDENTIALS']['PASSWORD']
    _cafile = SETTINGS['CERTIFICATES']['CAFILE']
    transport = SETTINGS['TRANSPORT']
    broker = SETTINGS['BROKER_INFO']['BROKER']
    port = SETTINGS['BROKER_INFO']['PORT']

    def __init__(self, environment: Environment):
        super().__init__(environment)
        self.client: MqttClient = MqttClient(
            environment=self.environment,
            transport=self.transport
        )

        # Check if Cafile is provide
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
    global CLIENT_COUNTER

    def __init__(
        self,
        *args,
        environment: Environment,
        client_id: typing.Optional[str] = None,
        **kwargs,
    ):
        if not client_id:
            self.client_id = f"locust-{CLIENT_COUNTER}"
            increase()
        else:
            self.client_id = client_id

        super().__init__(*args, client_id=self.client_id, **kwargs)
        self.environment = environment
        self.on_publish = self._on_publish_cb
        self.on_subscribe = self._on_subscribe_cb
        self.on_disconnect = self._on_disconnect_cb
        self.on_connect = self._on_connect_cb
        # self.on_message = self._on_message_cb

        self._publish_requests = {}
        self._subscribe_requests = {}
    
    def _on_message_cb(self, client, userdata, message):
        """
        If a message is retrieved from the subscribed topic, this function is 
        executed and logs the corresponding message (payload).
        """
        print(f'Recieved from {client.client_id} || {round(time.time(),1)} || {message.topic} ({str(message.qos)}) => {str(message.payload.decode())}')

    def _on_publish_cb(
        self,
        client: mqtt.Client,
        userdata: typing.Any,
        mid: int,
    ):
        cb_time = time.time()
        try:
            request_context = self._publish_requests.pop(mid)
        except KeyError:
            self.environment.events.request.fire(
                request_type=RequestType.MQTT.value,
                name=EventType.PUBLISH.value,
                response_time=0,
                response_length=0,
                exception=AssertionError(f"Could not find message data for mid '{mid}' in _on_publish_cb."),
                context={
                    "client_id": self.client_id,
                    "mid": mid,
                },
            )
        else:
            self.environment.events.request.fire(
                request_type=RequestType.MQTT.value,
                name=_generate_mqtt_event_name(
                    self.client_id,
                    EventType.PUBLISH.value, 
                    request_context.qos, 
                    request_context.topic),
                response_time=(cb_time - request_context.start_time) * 1000,
                response_length=request_context.payload_size,
                exception=None,
                context={
                    "client_id": self.client_id,
                    **request_context._asdict(),
                },
            )
        

    def _on_subscribe_cb(
        self,
        client: mqtt.Client,
        userdata: typing.Any,
        mid: int,
        granted_qos: list[int],
    ):
        cb_time = time.time()
        try:
            request_context = self._subscribe_requests.pop(mid)
        except KeyError:
            self.environment.events.request.fire(
                request_type=RequestType.MQTT.value,
                name=EventType.SUBSCRIBE.value,
                response_time=0,
                response_length=0,
                exception=AssertionError(f"Could not find message data for mid '{mid}' in _on_subscribe_cb."),
                context={
                    "client_id": self.client_id,
                    "mid": mid,
                },
            )
        else:
            if SUBACK_FAILURE in granted_qos:
                self.environment.events.request.fire(
                    request_type=RequestType.MQTT.value,
                    name=_generate_mqtt_event_name(
                        self.client_id,
                        EventType.SUBSCRIBE.value,
                        request_context.qos,
                        request_context.topic),
                    response_time=(cb_time - request_context.start_time) * 1000,
                    response_length=0,
                    exception=AssertionError(f"Broker returned an error response during subscription: {granted_qos}"),
                    context={
                        "client_id": self.client_id,
                        **request_context._asdict(),
                    },
                )
            else:
                self.environment.events.request.fire(
                    request_type=RequestType.MQTT.value,
                    name=_generate_mqtt_event_name(
                        self.client_id,
                        EventType.SUBSCRIBE.value,
                        request_context.qos,
                        request_context.topic),
                    response_time=(cb_time - request_context.start_time) * 1000,
                    response_length=0,
                    exception=None,
                    context={
                        "client_id": self.client_id,
                        **request_context._asdict(),
                    },
                )
        

    def _on_disconnect_cb(
        self,
        client: mqtt.Client,
        userdata: typing.Any,
        rc: int,
    ):
        if rc != 0:
            self.environment.events.request.fire(
                request_type=RequestType.MQTT.value,
                name=EventType.DISCONNECT.value,
                response_time=0,
                response_length=0,
                exception=rc,
                context={
                    "client_id": self.client_id,
                },
            )
        else:
            self.environment.events.request.fire(
                request_type=RequestType.MQTT.value,
                name=EventType.DISCONNECT.value,
                response_time=0,
                response_length=0,
                exception=None,
                context={
                    "client_id": self.client_id,
                },
            )
            print(f"Disconnected successfully: {self.client_id}")
        

    def _on_connect_cb(
        self,
        client: mqtt.Client,
        userdata: typing.Any,
        flags: dict[str, int],
        rc: int,
    ):
        if rc != 0:
            self.environment.events.request.fire(
                request_type=RequestType.MQTT.value,
                name=EventType.CONNECT.value,
                response_time=0,
                response_length=0,
                exception=rc,
                context={
                    "client_id": self.client_id,
                },
            )
        else:
            self.environment.events.request.fire(
                request_type=RequestType.MQTT.value,
                name=EventType.CONNECT.value,
                response_time=0,
                response_length=0,
                exception=None,
                context={
                    "client_id": self.client_id,
                },
            )

    def publish(
        self,
        topic: str,
        payload: typing.Optional[Any] = None,
        qos: int = 0,
        retain: bool = False,
        properties: typing.Optional[Properties] = None,
    ):
        request_context = PublishedMessageContext(
            qos=qos,
            topic=topic,
            start_time=time.time(),
            payload_size=payload if payload else 0,
        )

        publish_info = super().publish(topic, payload=payload, qos=qos, retain=retain)

        if publish_info.rc != mqtt.MQTT_ERR_SUCCESS:
            self.environment.events.request.fire(
                request_type=RequestType.MQTT.value,
                name=_generate_mqtt_event_name(
                    self.client_id,
                    EventType.PUBLISH.value,
                    request_context.qos,
                    request_context.topic),
                response_time=0,
                response_length=0,
                exception=publish_info.rc,
                context={
                    "client_id": self.client_id,
                    **request_context._asdict(),
                },
            )
        else:
            self._publish_requests[publish_info.mid] = request_context

    def subscribe(
        self,
        topic: str,
        qos: int = 0,
        options: typing.Optional[SubscribeOptions] = None,
        properties: typing.Optional[Properties] = None,
    ):
        request_context = SubscribeContext(
            qos=qos,
            topic=topic,
            start_time=time.time(),
        )

        result, mid = super().subscribe(topic=topic, qos=qos)
        if result != mqtt.MQTT_ERR_SUCCESS:
            self.environment.events.request.fire(
                request_type=RequestType.MQTT.value,
                name=_generate_mqtt_event_name(
                    self.client_id,
                    EventType.SUBSCRIBE.value,
                    request_context.qos,
                    request_context.topic),
                response_time=0,
                response_length=0,
                exception=result,
                context={
                    "client_id": self.client_id,
                    **request_context._asdict(),
                },
            )
        else:
            self._subscribe_requests[mid] = request_context
