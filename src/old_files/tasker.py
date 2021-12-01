from locust import TaskSet, between,task
import string
import random
import time


# broker = 'localhost'
broker = 'testingmqtt.cloud.shiftr.io'
port = 1883
topics = ['/home', 
        '/home/room1',
        '/home/room1/temp',
        '/home/room1/humid',
        '/home/room2',
        '/home/room2/temp',
        '/home/room2/humid',
        '/home/room3',
        '/home/room3/temp',
        '/home/room3/humid',
        '/office', 
        '/office/room1',
        '/office/room1/temp',
        '/office/room1/humid',
        '/office/room2',
        '/office/room2/temp',
        '/office/room2/humid',
        '/office/room3',
        '/office/room3/temp',
        '/office/room3/humid',
]


class TaskerManager(TaskSet):
    '''
    This class is responsible to create methods for tasks, which 
    will be assinged randomly with weights on objects.
    '''

    wait_time = between(0.01,0.1)

    def on_start(self):
        '''
        On start the client connects to the broker
        '''
        self.client.connect(broker, port)
        self.client.disconnect()

    def start_connection(self):
        self.client.reconnect()
        self.client.loop_start()
    
    def get_payload(self, id):
        return f'Task{id}:{random.randint(0,9)}'

    def stop_connection(self):
        self.client.disconnect()
        self.client.loop_stop()
        time.sleep(1)

    def on_message(self, mq, userdata, message):
        '''Decode message received'''
        print(f'Received: ({message.payload.decode()}) from {message.topic}')

    @task(1)
    def publish_task1(self):
        '''Publish task 1'''
        self.start_connection()
        result = self.client.publish(random.choice(topics), self.get_payload(id=1))
        result.wait_for_publish()
        self.stop_connection()

    @task(1)
    def publish_task2(self):
        '''Publish task 2'''
        self.start_connection()
        result = self.client.publish(random.choice(topics), self.get_payload(id=2))
        result.wait_for_publish()
        self.stop_connection()
    
    @task(1)
    def subscribe_task3(self):
        '''Subscribe task 3'''
        self.start_connection()
        self.client.subscribe(random.choice(topics))
        self.client.on_message = self.on_message
        self.client.loop_forever()