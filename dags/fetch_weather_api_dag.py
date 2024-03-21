from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator

from dags.fetch_weather_api import retrieve_weather_data

with DAG(
    "weather_data",
    start_date=pendulum.yesterday(),
    description="A task to upload weather data daily",
    catchup=False,
    schedule_interval="@daily",
    default_args={"depends_on_past": False},
) as dag:

    get_weather_data_and_upload_to_bq = PythonOperator(
        task_id="get_weather_data_and_upload_to_bq_task",
        python_callable=retrieve_weather_data,
        op_kwargs={"date": "2024-03-20"},
    )

    get_weather_data_and_upload_to_bq
