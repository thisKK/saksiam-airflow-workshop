from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils import timezone

default_args = {
    "ownner" : "kay",
    "start_date": timezone.datetime(2021, 9, 27)
}

with DAG(
    'homework_1',
    default_args=default_args,
    schedule_interval=None,
    tags=['homework'],
) as dag:

    one = DummyOperator(task_id="1")
    two = DummyOperator(task_id="2")
    three = DummyOperator(task_id="3")
    four = DummyOperator(task_id="4")
    five = DummyOperator(task_id="5")
    six = DummyOperator(task_id="6")
    seven = DummyOperator(task_id="7")
    eight = DummyOperator(task_id="8")
    nine = DummyOperator(task_id="9")

    one >> [two ,five] >> six >> eight >> nine
    two >> three >> four >> nine
    five >> [six,seven] >> eight >> nine