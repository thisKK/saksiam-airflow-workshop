from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
import logging

def _say_hello(): 
    print("say hello")

def _print_log_massages():
    logging.info("print log")

default_args = {
    "ownner" : "kay",
    "start_date": timezone.datetime(2021, 9, 27)
}

with DAG(
    'homework_2',
    default_args=default_args,
    schedule_interval="*/30 * * * *",
    tags=['homework'],
     catchup=False,

) as dag:
    start = DummyOperator(task_id="start")

    echo_hello  = BashOperator(
        task_id="echo_hello",
        bash_command='echo "hello"',
    )

    say_hello = PythonOperator(
        task_id="say_hello",
        python_callable=_say_hello,
    )

    print_log_massages = PythonOperator(
        task_id="print_log_massages",
        python_callable= _print_log_massages,

    )

    end = DummyOperator(task_id="end")


    start >> echo_hello  >> say_hello >> print_log_massages >> end