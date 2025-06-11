from mlflow.tracking import MlflowClient
import mlflow
import shutil
import os
import pandas as pd
import json


# https://support.hpe.com/hpesc/public/docDisplay?docId=sf000104615en_us&docLocale=en_US
class client:

    def __init__(self, url, s3_url, exp_name, token):
        AWS_ENDPOINT_URL = s3_url
        MLFLOW_S3_ENDPOINT_URL = AWS_ENDPOINT_URL
        os.environ["MLFLOW_S3_IGNORE_TLS"] = "true"
        os.environ["MLFLOW_TRACKING_INSECURE_TLS"] = "true"
        os.environ["MLFLOW_TRACKING_TOKEN"] = token
        os.environ["AUTH_TOKEN"] = token
        os.environ["MLFLOW_S3_ENDPOINT_URL"] = MLFLOW_S3_ENDPOINT_URL
        os.environ["MLFLOW_TRACKING_URI"] = url
        os.environ["AWS_ENDPOINT_URL"] = AWS_ENDPOINT_URL
        os.environ[
            "AWS_ACCESS_KEY_ID"] = token  # Ensure correct AWS access key
        os.environ["AWS_SECRET_ACCESS_KEY"] = "s3"  # Ensure correct secret key

        #self.client = MlflowClient(tracking_uri="http://mlflow.mlflow.svc.cluster.local:5000")
        self.client = MlflowClient()
        experiment = self.client.get_experiment_by_name(exp_name)
        runs = self.client.search_runs(
            experiment_ids=[experiment.experiment_id],
            order_by=["start_time DESC"],
            max_results=1)

        self.run = runs[0]
        self.run_id = self.run.info.run_id
        self.artifact_uri = self.run.info.artifact_uri

    def get_artifact_csv_bulk(self, artifact_list):
        path_list = []
        temp_path = mlflow.artifacts.download_artifacts(self.artifact_uri)
        csv_data_dict = {}
        for path in artifact_list:
            csv_data_dict[path] = pd.read_csv(f"{temp_path}/{path}")

        return csv_data_dict

    def upload_metric(self, drift_metrics):
        with mlflow.start_run(run_id=self.run.info.run_id):
            for metric in drift_metrics:
                mlflow.log_metric(metric['name'], metric['value'])
