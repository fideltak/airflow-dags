import datetime

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator


@dag(start_date=datetime.datetime(2021, 1, 1), schedule="@daily")
def Test05_test_dag():
    EmptyOperator(task_id="task")


Test05_test_dag()
