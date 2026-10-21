"""Daily 02:00 Asia/Ho_Chi_Minh retraining, via a typed internal worker API."""
import json
import os
import urllib.request

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor


def submit(**context):
    base = os.getenv("JOB_WORKER_URL", "http://job-worker:9000").rstrip("/")
    body = json.dumps({"request_id": context["dag_run"].run_id, "quick": True}).encode()
    request = urllib.request.Request(base + "/jobs/retrain", data=body, headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(request, timeout=30) as response:
        result = json.load(response)
    if result.get("status") == "failed":
        raise AirflowException(result.get("error", "Retraining failed"))
    return result["job_id"]


def completed(**context):
    identifier = context["ti"].xcom_pull(task_ids="submit_retraining")
    base = os.getenv("JOB_WORKER_URL", "http://job-worker:9000").rstrip("/")
    with urllib.request.urlopen(base + "/jobs/" + identifier, timeout=30) as response:
        result = json.load(response)
    if result["status"] == "failed":
        raise AirflowException(result.get("error", "Retraining failed"))
    return result["status"] == "completed"


with DAG(
    dag_id="retailrocket_daily_retrain",
    description="Snapshot accepted events, rebuild lake/features, evaluate ALS and atomically activate model",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 10, 21, tz="Asia/Ho_Chi_Minh"),
    catchup=False, max_active_runs=1,
    default_args={"retries": 1}, tags=["phase6", "retailrocket"],
) as dag:
    launch = PythonOperator(task_id="submit_retraining", python_callable=submit)
    wait = PythonSensor(task_id="wait_for_evaluated_model", python_callable=completed,
                        poke_interval=30, timeout=7200, mode="reschedule")
    launch >> wait
