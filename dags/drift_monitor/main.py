from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.sdk import dag, task
from airflow.models import Variable
from drift_monitor.pkg import s3, token, mlflow, whylog, slack
from drift_monitor.internal import rental_house
import logging

logger = logging.getLogger(__name__)


def dag_tasks(config):
    #Params
    keycloak_addr = config['keycloak_addr']
    keycloak_user_name = config['keycloak_user_name']
    keycloak_user_password = config['keycloak_user_password']
    s3_url = config['s3_url']
    s3_base_prefix = config['s3_base_prefix']
    s3_bucket = config['s3_bucket']
    model_name = config['model_name']
    target_date = config['target_date']
    if target_date == "today":
        target_date = datetime.now().strftime("%Y%m%d")
    mlflow_url = config['mlflow_url']
    mlflow_s3_url = config['mlflow_s3_url']
    mlflow_exp_name = config['mlflow_exp_name']
    mlflow_artifact_list = config['mlflow_artifact_list']
    slack_token = config['slack_token']
    slack_channel = config['slack_channel']

    @task(do_xcom_push=True, multiple_outputs=True)
    def generate_token(**context):
        access_token = token.generate_by_user_password(keycloak_addr,
                                                       keycloak_user_name,
                                                       keycloak_user_password)
        return {'access_token': access_token}

    @task(do_xcom_push=True, multiple_outputs=True)
    def list_trace_files(**context):
        access_token = context["ti"].xcom_pull(task_ids="generate_token",
                                               key="access_token")
        c = s3.client(s3_url, access_token)
        s3_prefix = f"{s3_base_prefix}/{model_name}/{target_date}"
        obj_key_list = c.list_trace_files(s3_bucket, s3_prefix)
        logger.info(f'File List')
        for key in obj_key_list:
            logger.info(f'{key}')
        return {'obj_key_list': obj_key_list}

    @task(do_xcom_push=True, multiple_outputs=True)
    def get_trace_files(**context):
        access_token = context["ti"].xcom_pull(task_ids="generate_token",
                                               key="access_token")
        obj_key_list = context["ti"].xcom_pull(task_ids="list_trace_files",
                                               key="obj_key_list")
        c = s3.client(s3_url, access_token)
        json_data_list = c.get_obj_bulk(s3_bucket, obj_key_list)
        return {'json_data_list': json_data_list}

    @task(do_xcom_push=True, multiple_outputs=True)
    def trace_file_processing(**context):
        json_data_list = context["ti"].xcom_pull(task_ids="get_trace_files",
                                                 key="json_data_list")
        trace_query_df, trace_pred_df = rental_house.data().trace_preparation(
            json_data_list)
        return {
            'trace_query_df': trace_query_df,
            'trace_pred_df': trace_pred_df
        }

    @task(do_xcom_push=True, multiple_outputs=True)
    def get_mlflow_artifacts(**context):
        access_token = context["ti"].xcom_pull(task_ids="generate_token",
                                               key="access_token")
        c = mlflow.client(mlflow_url, mlflow_s3_url, mlflow_exp_name,
                          access_token)
        csv_data_dict = c.get_artifact_csv_bulk(mlflow_artifact_list)
        train_df = csv_data_dict[mlflow_artifact_list[0]]
        train_target_df = train_df[["monthly_rent"]].copy()
        train_query_df = train_df.drop(columns=["monthly_rent"], axis=1)
        return {
            'train_query_df': train_query_df,
            'train_target_df': train_target_df
        }

    @task(do_xcom_push=True, multiple_outputs=True)
    def check_data_drift(**context):
        train_query_df = context["ti"].xcom_pull(
            task_ids="get_mlflow_artifacts", key="train_query_df")
        trace_query_df = context["ti"].xcom_pull(
            task_ids="trace_file_processing", key="trace_query_df")
        drift_metrics = whylog.check_data_drift(train_query_df, trace_query_df)

        return {'drift_metrics': drift_metrics}

    @task
    def upload_result(**context):
        access_token = context["ti"].xcom_pull(task_ids="generate_token",
                                               key="access_token")
        drift_metrics = context["ti"].xcom_pull(task_ids="check_data_drift",
                                                key="drift_metrics")
        c = mlflow.client(mlflow_url, mlflow_s3_url, mlflow_exp_name,
                          access_token)
        csv_data_dict = c.upload_metric(drift_metrics)

    @task
    def slack_notify(**context):
        drift_metrics = context["ti"].xcom_pull(task_ids="check_data_drift",
                                                key="drift_metrics")
        if slack_token and slack_channel:
            c = slack.client(slack_token)
            msg = f"Model Name: {model_name} \nDate: {target_date} \n"
            for metric in drift_metrics:
                msg = msg + f"{metric['name']}: {metric['value']}\n"
            c.notify(slack_channel, msg)

    generate_token() >> list_trace_files() >> get_trace_files(
    ) >> trace_file_processing() >> get_mlflow_artifacts() >> check_data_drift(
    ) >> upload_result() >> slack_notify()


@dag(dag_id="model01_drift_monitor",
     start_date=datetime(2021, 1, 1),
     schedule="@daily",
     catchup=False)
def model01_drift_monitor():
    config = Variable.get("rental_house_drift_monitor_model01",
                          deserialize_json=True)
    dag_tasks(config)


@dag(dag_id="model02_drift_monitor",
     start_date=datetime(2021, 1, 1),
     schedule="@daily",
     catchup=False)
def model02_drift_monitor():
    config = Variable.get("rental_house_drift_monitor_model02",
                          deserialize_json=True)
    dag_tasks(config)


dag_model01 = model01_drift_monitor()
dag_model02 = model02_drift_monitor()
