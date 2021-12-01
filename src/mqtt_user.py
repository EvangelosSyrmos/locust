from __future__ import annotations

import random
import time
import typing
import json

from locust import User
from locust.env import Environment

from uuid import uuid4
from typing import Any, Optional, List
from enum import Enum
from dataclasses import dataclass

import paho.mqtt.client as mqtt

if typing.TYPE_CHECKING:
    from paho.mqtt.properties import Properties
    from paho.mqtt.subscribeoptions import SubscribeOptions


# A SUBACK response for MQTT can only contain 0x00, 0x01, 0x02, or 0x80. 0x80
# indicates a failure to subscribe.
#
# http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Figure_3.26_-
SUBACK_FAILURE = 0x80
REQUEST_TYPE = "MQTT"

class EventType(Enum):
    CONNECT = 'connect'
    DISCONNECT ='disconnect'
    PUBLISH = 'publish'
    SUBSCRIBE = 'subscribe'


class RequestType(Enum):
    MQTT = 'MQTT'


def _generate_mqtt_event_name(event_type: str, qos: int, topic: str):
    return f"{event_type}:{qos}:{topic}"


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
    abstract = True

    with open('settings.json') as f:
        SETTINGS = json.load(f)

    username = SETTINGS['USERNAME']
    password = SETTINGS['PASSWORD']
    _cafile = SETTINGS['CAFILE']
    transport = SETTINGS['TRANSPORT']
    broker = SETTINGS['BROKER']
    port = SETTINGS['PORT']

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
    def __init__(
        self,
        *args,
        environment: Environment,
        client_id: typing.Optional[str] = None,
        **kwargs,
    ):

        if not client_id:
            self.client_id = f"locust-{uuid4()}"
        else:
            self.client_id = client_id

        super().__init__(*args, client_id=self.client_id, **kwargs)
        self.environment = environment
        self.on_publish = self._on_publish_cb
        self.on_subscribe = self._on_subscribe_cb
        self.on_disconnect = self._on_disconnect_cb
        self.on_connect = self._on_connect_cb
        self.on_message = self._on_message_cb

        self._publish_requests = {}
        self._subscribe_requests = {}
    
    def _on_message_cb(self, client, userdata, message):
        """
        If a message is retrieved from the subscribed topic, this function is 
        executed and logs the corresponding message (payload).
        """
        print(f'{str(message.payload.decode())} -> {message.topic} | {str(message.qos)}')

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
    