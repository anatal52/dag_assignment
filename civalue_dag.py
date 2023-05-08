from __future__ import annotations

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
import pendulum, datetime, logging, os
from pprint import pprint

with DAG(
    "civalue_dag",
    schedule=None,
    start_date=pendulum.datetime(2023, 5, 1, tz="UTC"),
    catchup=False,
    params={
        'environment_type': '{{ dag_run.conf["environment_type"] }}'
    }
) as dag:
    
    def check_env(**kwargs):
        # parses the environment type parameter given by the user and chooses the correct workflow branch to execute
        env = kwargs["params"].get("environment_type")
        if env == "development":
            return "write_file_development"
        elif env == "production":
            return "write_file_production"
        else:
            raise ValueError(f"Invalid environment_type '{env}'")

    def write_file(**kwargs):
        # printing text into local file
        env = kwargs["params"].get("environment_type")
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"civalue_{env}_{timestamp}.txt"
        file_content = f"hello ciValue from {env} branch"

        file_path = os.path.join(os.getcwd(), file_name)
        with open(file_path, "w") as f:
            f.write(file_content)
        
        logging.info(f"file_path: {file_path}")
        return file_path

    def print_file(**kwargs):
        # reads the newly created file and prints its content to the console
        env = kwargs["params"].get("environment_type")
        file_path = kwargs["ti"].xcom_pull(task_ids=f"write_file_{env}")
        logging.info(f"file_path: {file_path}")
        with open(file_path, 'r') as file:
            file_content = file.read()
        logging.info(f"{file_content}")

    env = BranchPythonOperator(
        task_id="check_env",
        python_callable=check_env,
    )

    dev = PythonOperator(
        task_id="write_file_development",
        python_callable=write_file
    )
    
    prod = PythonOperator(
        task_id="write_file_production",
        python_callable=write_file
    )

    print = PythonOperator(
        task_id="print_file",
        python_callable=print_file,
        trigger_rule="none_failed"
    )

    env >> [dev,prod] >> print
