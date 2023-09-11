import datetime

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator


@dag(start_date=datetime.datetime(2021, 1, 1), schedule="@daily")
def Test04_dag():
    EmptyOperator(task_id="task")


Test04_dag()
