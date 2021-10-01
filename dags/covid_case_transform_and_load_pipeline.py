from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.email import EmailOperator
from airflow.models import Variable 

from airflow.utils import timezone
import logging
import requests
import csv

DAG_FOLDER = "/opt/airflow/dags"


def _load_data_from_data_lake(datestamp):
    hook = S3Hook(aws_conn_id="s3_conn")
    temp_file = hook.download_file(
        key=f"covid/{datestamp}/{datestamp}-covid-cases.csv",
        bucket_name="landing",
    )
    return temp_file

def _insert_data_to_db(ti):
    hook = PostgresHook(postgres_conn_id="db_conn")
    filename = ti.xcom_pull(task_ids="load_data_from_data_lake")

    connection = hook.get_conn()
    cursor = connection.cursor()

    with open(filename, "r") as f:
        reader = csv.reader(f)
        for row in reader:
            print(row)

            txn_date = row[0]
            new_case = row[1]
            query = f"""
                INSERT INTO covid (txn_date, new_case) VALUES ('{txn_date}', {new_case})
                ON CONFLICT (txn_date) DO UPDATE SET new_case = {new_case}
            """
            logging.info(query)
            cursor.execute(query)
            connection.commit()

    cursor.close()
    connection.close()

default_args = {
    "ownner" : "kay",
    "start_date": timezone.datetime(2021, 9, 27)
}

with DAG(
    'covid_case_transform_and_load_pipeline',
    default_args=default_args,
    schedule_interval="@daily",
    tags=['saksiam'],
) as dag:

    start = DummyOperator(task_id="start")


    check_file = S3KeySensor(
        task_id="check_file",
        bucket_name="landing",
        bucket_key="covid/{{ ds }}/{{ ds }}-covid-cases.csv",
        aws_conn_id="s3_conn",
    )

    load_data_from_data_lake = PythonOperator(
        task_id="load_data_from_data_lake",
        python_callable=_load_data_from_data_lake,
        op_kwargs = {
            "datestamp" : "{{ ds }}",
        },
        # op_args=[1, 2],
    )

    create_table = PostgresOperator(
        task_id="create_table",
        sql="""
            CREATE TABLE IF NOT EXISTS covid (
                txn_date TEXT UNIQUE,
                new_case INT
            )
        """,
        postgres_conn_id="db_conn",
    )

    insert_data_to_db = PythonOperator(
        task_id="insert_data_to_db",
        python_callable=_insert_data_to_db,
    )

    notify_by_email = EmailOperator(
        task_id="notify_by_email",
        to=["piampoon.kay@odds.team"],
        cc=["piampoon.kay@odds.team"],
        subject="{{ var.value.covid_email_subject }}", #email_subject = Variable.get("covid_email_subject")
        html_content="""
            <h1>Your COVID Pipeline Finished!</h1>
            <p>Done.</p>
        """,
    )
    
    end = DummyOperator(task_id="end")

    start >> check_file >> load_data_from_data_lake >> create_table >> insert_data_to_db >> notify_by_email >> end
    
