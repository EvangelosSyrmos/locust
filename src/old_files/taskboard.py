import time
import random
from locust import task, TaskSet, between

topics = ['/home', 
        '/home/room1',
        '/home/room1/temp',
        '/home/room1/humID',
        '/home/room2',
        '/home/room2/temp',
        '/home/room2/humID',
        '/home/room3',
        '/home/room3/temp',
        '/home/room3/humID',
        '/office', 
        '/office/room1',
        '/office/room1/temp',
        '/office/room1/humID',
        '/office/room2',
        '/office/room2/temp',
        '/office/room2/humID',
        '/office/room3',
        '/office/room3/temp',
        '/office/room3/humID',
]


class TaskBoard(TaskSet):
    ''' This class is only used for generating
    the tasks for every user to randomly execute.
    '''
    wait_time = between(0.01, 0.1)

    def on_start(self):
        '''
        The mandatory duration needed for client
        to connect.
        '''
        time.sleep(5)
    
    def get_payload(self):
        ''' Generate random payload'''
        payload = bytes(f'Task:{random.randint(0,9)}', 'utf-8')
        return payload
    
    @task(1)
    def task1(self):
        ''' This task is just for testing perposuses'''
        self.client.publish(random.choice(topics), self.get_payload())
    
    # @task(2)
    # def task2(self):
    #     ''' This task is just for testing perposuses'''
    #     self.client.publish(random.choice(topics), self.get_payload())

