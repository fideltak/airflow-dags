from datetime import datetime
from airflow.decorators import dag, task
from airflow.models import Variable
from drift_monitor.pkg import s3, token, mlflow, whylog, slack
from drift_monitor.internal import rental_house
import logging

logger = logging.getLogger(__name__)

def dag_tasks(config, model_name):
    keycloak_addr = config['keycloak_addr']
    keycloak_user_name = config['keycloak_user_name']
    keycloak_user_password = config['keycloak_user_password']
    s3_url = config['s3_url']
    s3_base_prefix = config['s3_base_prefix']
    s3_bucket = config['s3_bucket']
    target_date = config['target_date']
    if target_date == "today":
        target_date = datetime.now().strftime("%Y%m%d")
    mlflow_url = config['mlflow_url']
    mlflow_s3_url = config['mlflow_s3_url']
    mlflow_exp_name = config['mlflow_exp_name']
    mlflow_artifact_list = config['mlflow_artifact_list']
    slack_token = config['slack_token']
    slack_channel = config['slack_channel']

    @task(task_id=f"generate_token_{model_name}", multiple_outputs=True)
    def generate_token():
        access_token = token.generate_by_user_password(
            keycloak_addr, keycloak_user_name, keycloak_user_password)
        return {'access_token': access_token}

    @task(task_id=f"list_trace_files_{model_name}", multiple_outputs=True)
    def list_trace_files(access_token):
        c = s3.client(s3_url, access_token)
        s3_prefix = f"{s3_base_prefix}/{model_name}/{target_date}"
        obj_key_list = c.list_trace_files(s3_bucket, s3_prefix)
        logger.info(f'File List: {obj_key_list}')
        return {'obj_key_list': obj_key_list}

    @task(task_id=f"get_trace_files_{model_name}", multiple_outputs=True)
    def get_trace_files(access_token, obj_key_list):
        c = s3.client(s3_url, access_token)
        json_data_list = c.get_obj_bulk(s3_bucket, obj_key_list)
        return {'json_data_list': json_data_list}

    @task(task_id=f"trace_file_processing_{model_name}", multiple_outputs=True)
    def trace_file_processing(json_data_list):
        trace_query_df, trace_pred_df = rental_house.data().trace_preparation(json_data_list)
        return {
            'trace_query_df': trace_query_df,
            'trace_pred_df': trace_pred_df
        }

    @task(task_id=f"get_mlflow_artifacts_{model_name}", multiple_outputs=True)
    def get_mlflow_artifacts(access_token):
        c = mlflow.client(mlflow_url, mlflow_s3_url, mlflow_exp_name, access_token)
        csv_data_dict = c.get_artifact_csv_bulk(mlflow_artifact_list)
        train_df = csv_data_dict[mlflow_artifact_list[0]]
        train_target_df = train_df[["monthly_rent"]].copy()
        train_query_df = train_df.drop(columns=["monthly_rent"], axis=1)
        return {
            'train_query_df': train_query_df,
            'train_target_df': train_target_df
        }

    @task(task_id=f"check_data_drift_{model_name}", multiple_outputs=True)
    def check_data_drift(train_query_df, trace_query_df):
        drift_metrics = whylog.check_data_drift(train_query_df, trace_query_df)
        return {'drift_metrics': drift_metrics}

    @task(task_id=f"upload_result_{model_name}")
    def upload_result(access_token, drift_metrics):
        c = mlflow.client(mlflow_url, mlflow_s3_url, mlflow_exp_name, access_token)
        c.upload_metric(drift_metrics)

    @task(task_id=f"slack_notify_{model_name}")
    def slack_notify(drift_metrics):
        if slack_token and slack_channel:
            c = slack.client(slack_token)
            msg = f"Model Name: {model_name} \nDate: {target_date} \n"
            for metric in drift_metrics:
                msg += f"{metric['name']}: {metric['value']}\n"
            c.notify(slack_channel, msg)

    token_out = generate_token()
    trace_files = list_trace_files(token_out['access_token'])
    jsons = get_trace_files(token_out['access_token'], trace_files['obj_key_list'])
    processed = trace_file_processing(jsons['json_data_list'])
    ml_artifacts = get_mlflow_artifacts(token_out['access_token'])
    drift = check_data_drift(ml_artifacts['train_query_df'], processed['trace_query_df'])
    upload_result(token_out['access_token'], drift['drift_metrics']) >> slack_notify(drift['drift_metrics'])

@dag(dag_id="model01_drift_monitor", start_date=datetime(2021, 1, 1), schedule="@daily", catchup=False)
def model01_drift_monitor():
    config = Variable.get("rental_house_drift_monitor_model01", deserialize_json=True)
    dag_tasks(config, model_name="model01")

@dag(dag_id="model02_drift_monitor", start_date=datetime(2021, 1, 1), schedule="@daily", catchup=False)
def model02_drift_monitor():
    config = Variable.get("rental_house_drift_monitor_model02", deserialize_json=True)
    dag_tasks(config, model_name="model02")

dag_model01 = model01_drift_monitor()
dag_model02 = model02_drift_monitor()