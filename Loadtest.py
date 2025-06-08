from locust import HttpUser, task, between


class WebsiteUser(HttpUser):
    wait_time = between(60, 61)  # Each user waits for approximately 1 minute

    @task
    def load_test(self):
        self.client.get("/")


"""
In the terminal, enter the following command to run the load test:
locust -f Loadtest.py --csv=results/loadtest

Go to http://localhost:8089/ to see the load test in action.
"""
