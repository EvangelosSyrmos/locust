import json
import random
from locust import task, TaskSet
from locust.user.wait_time import between
from client import MqttUser

with open('settings.json') as f:
    SETTINGS = json.load(f)

class MyTasks(TaskSet):

    # Duration between tasks
    wait_time = between(SETTINGS['LOWER'], SETTINGS['UPPER'])

    # def on_start(self):
        # time.sleep(3)

    # Task(2) -> Twice the probability of occurance 
    @task(2)
    def test_publish(self):
        """
        This task publishes to a random topic
        """
        self.client.publish(
            random.choice(
            SETTINGS['TOPICS']),
            payload=random.uniform(0,1)
        )
    
    # Task(1) -> Twice the probability of occurance
    @task(1)
    def test_subscribe(self):
        """
        This task subscribes to a random topic
        """
        self.client.subscribe(
            random.choice(SETTINGS['TOPICS']),
            qos=0
        )

class MyTestUser(MqttUser):
    tasks = {MyTasks}