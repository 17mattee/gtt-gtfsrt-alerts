import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from locust import HttpUser, task, between

class GTTUser(HttpUser):
    wait_time = between(1, 3)

    @task(3)
    def get_gtfs_rt_feed(self):
        self.client.get("/gtfs-rt/alerts.pb")

    @task(1)
    def get_dashboard(self):
        self.client.get("/")

    @task(1)
    def get_health(self):
        self.client.get("/health")
