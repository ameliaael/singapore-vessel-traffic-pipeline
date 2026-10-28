from datetime import datetime
import time

import requests
from airflow.sdk import dag, task
from airflow.hooks.base import BaseHook


@dag(
    dag_id="vessel_traffic_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["vessel-traffic", "portfolio"],
)
def vessel_traffic_pipeline():

    @task
    def run_databricks_job():
        conn = BaseHook.get_connection("databricks_default")
        token = conn.password
        host = conn.host.rstrip("/")

        headers = {"Authorization": f"Bearer {token}"}

        response = requests.post(
            f"{host}/api/2.1/jobs/run-now",
            headers=headers,
            json={"job_id": 515929156113460},
            timeout=30,
        )
        response.raise_for_status()

        run_id = response.json()["run_id"]
        print(f"Databricks run started: {run_id}")

        deadline = time.time() + 3600

        while time.time() < deadline:
            response = requests.get(
                f"{host}/api/2.1/jobs/runs/get",
                headers=headers,
                params={"run_id": run_id},
                timeout=30,
            )
            response.raise_for_status()

            state = response.json().get("state", {})
            life_cycle = state.get("life_cycle_state")
            result = state.get("result_state")

            print(f"Databricks status: {life_cycle}; result: {result}")

            if life_cycle == "TERMINATED":
                if result == "SUCCESS":
                    print("Databricks pipeline completed successfully!")
                    return

                raise RuntimeError(
                    f"Databricks job failed. Result: {result}; "
                    f"message: {state.get('state_message')}"
                )

            if life_cycle in ("INTERNAL_ERROR", "SKIPPED"):
                raise RuntimeError(f"Databricks run ended unexpectedly: {state}")

            time.sleep(20)

        raise TimeoutError(
            f"Databricks run {run_id} did not finish within one hour."
        )

    run_databricks_job()


vessel_traffic_pipeline()
