# -*- coding: utf-8 -*-
"""
dags/disney_common.py
=====================
2つの DAG が共通で使う設定と、Pod の起動まわりをまとめたもの。

★このファイルが import してよいもの
  airflow と標準ライブラリだけ。

  PCAI の Airflow は**共有環境**で、ワーカーのイメージを利用者が
  差し替えられない。そこに pandas / xgboost が入っている保証は無い。
  DAG ファイルはスケジューラが繰り返し読み込むので、ここで
  `import pandas` などをすると、**DAG が壊れて一覧にすら出てこない**。

  そのため実処理は一切書かず、
  「自前のイメージ(disney-jobs)を Pod として起動する」ことだけを行う。
  中身は disney/tasks.py にある。

★設定は Airflow Variables で変えられる
  イメージのタグや共有ボリュームの場所は環境ごとに違う。
  DAG を書き換えずに済むよう、Airflow の画面から変えられるようにする。
    Admin → Variables

    disney_image             例: registry.example.com/disney-jobs:2.0.0
    disney_shared_claim      例: kubeflow-shared-pvc
    disney_shared_subpath    例: disney
    disney_namespace         省略時は Pod が動いている namespace
    disney_image_pull_secret 例: imagepull
"""

from __future__ import annotations

import datetime
import os

import pendulum
from airflow.models import Variable

# ==========================================
# 基本設定
# ==========================================
JST = pendulum.timezone("Asia/Tokyo")

# ★収集・学習のスケジュールと営業時間
#   本来は disney/config.py が唯一の正だが、この DAG からは import できない
#   (共有 Airflow に依存が無いため)。そこで値をここに写している。
#   ずれると気づけないので、tests/check_pipeline.py が
#   config 側と一致することを検査している。
OPEN_HOUR = 9
CLOSE_HOUR = 21
SLOT_MINUTES = 30
RETRAIN_AT_HOUR = 22

COLLECTOR_SCHEDULE = "0,30 9-20 * * *"    # 9:00〜20:30 の30分毎
RETRAIN_SCHEDULE = "0 22 * * *"           # 毎日22時(閉園後)


def _var(name, default=None):
    """Airflow Variable を読む。無ければ環境変数、それも無ければ既定値。

    ★DAG のパース時に例外を投げない
      Variable が未登録でも DAG 自体は読み込めるようにする。
      ここで落とすと、設定漏れのときに DAG 一覧から消えてしまい、
      画面上で原因を確かめることすらできなくなる。
    """
    try:
        return Variable.get(name, default_var=None) or os.getenv(
            name.upper(), default)
    except Exception:
        return os.getenv(name.upper(), default)


# ==========================================
# 共有ボリューム
# ==========================================
# PCAI には全ユーザーのタスクから見える共有 PVC があり、
# Airflow のタスクからは /mnt/shared で参照できる。
# 収集CSVと学習成果物はここに置き、推論サービスも同じものを読む。
SHARED_CLAIM = _var("disney_shared_claim", "kubeflow-shared-pvc")
SHARED_SUBPATH = _var("disney_shared_subpath", "disney")

MOUNT_PATH = "/mnt/shared"
DATA_DIR = f"{MOUNT_PATH}/{SHARED_SUBPATH}/data"
MODEL_DIR = f"{MOUNT_PATH}/{SHARED_SUBPATH}/artifacts"

# ジョブ用イメージ
IMAGE = _var("disney_image", "disney-jobs:2.0.0")
IMAGE_PULL_POLICY = _var("disney_image_pull_policy", "IfNotPresent")
IMAGE_PULL_SECRET = _var("disney_image_pull_secret", "imagepull")
NAMESPACE = _var("disney_namespace")     # 省略時は Pod と同じ namespace

DEFAULT_ARGS = {
    "owner": "disney",
    "depends_on_past": False,
}


def make_task(task_id, arguments, memory="2Gi", cpu="1",
              retries=1, retry_delay_minutes=5, timeout_minutes=30,
              skip_on_exit_code=None, **kwargs):
    """タスク1つ分の KubernetesPodOperator を組み立てる。

    ★なぜ KubernetesPodOperator なのか
      共有 Airflow のワーカーには学習用の依存が無く、
      Executor のリソースも CPU 1 / メモリ 2Gi に固定されていて変えられない。
      別 Pod として起動すれば、必要な依存も必要なメモリも自分で決められる。

    Args:
        arguments: disney.tasks に渡す引数
                   例) ["collect", "--park", "tdl", "--slot", "{{ ... }}"]
        memory/cpu: この処理に必要な量。学習は多めに要る。
        skip_on_exit_code: この終了コードなら「スキップ」として扱う。
                           disney.tasks は 2 を「まだやることが無い」の意味で返す。
    """
    from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
    from kubernetes.client import models as k8s

    volume = k8s.V1Volume(
        name="shared",
        persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
            claim_name=SHARED_CLAIM),
    )
    mount = k8s.V1VolumeMount(name="shared", mount_path=MOUNT_PATH)

    env = {
        "DISNEY_DATA_DIR": DATA_DIR,
        "DISNEY_MODEL_DIR": MODEL_DIR,
        "TZ": "Asia/Tokyo",
        # 作業ディレクトリを実行ごとに分けるのに使う
        "AIRFLOW_RUN_ID": "{{ run_id }}",
    }

    params = dict(
        task_id=task_id,
        name=f"disney-{task_id}".replace("_", "-"),
        image=IMAGE,
        image_pull_policy=IMAGE_PULL_POLICY,
        arguments=list(arguments),
        env_vars=[k8s.V1EnvVar(name=k, value=v) for k, v in env.items()],
        volumes=[volume],
        volume_mounts=[mount],
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": cpu, "memory": memory},
            limits={"cpu": cpu, "memory": memory},
        ),
        # 実行のたびに Pod を作り直し、終わったら片付ける
        is_delete_operator_pod=True,
        # ★Pod を起動した Airflow 側から見えるように、中のログを引き上げる
        get_logs=True,
        log_events_on_failure=True,
        # disney.tasks が /airflow/xcom/return.json に結果を書く
        do_xcom_push=True,
        in_cluster=True,
        retries=retries,
        retry_delay=datetime.timedelta(minutes=retry_delay_minutes),
        execution_timeout=datetime.timedelta(minutes=timeout_minutes),
    )

    if NAMESPACE:
        params["namespace"] = NAMESPACE
    if IMAGE_PULL_SECRET:
        params["image_pull_secrets"] = [
            k8s.V1LocalObjectReference(name=IMAGE_PULL_SECRET)]
    if skip_on_exit_code is not None:
        params["skip_on_exit_code"] = skip_on_exit_code

    params.update(kwargs)
    return KubernetesPodOperator(**params)


def slot_template():
    """収集スロットとして渡すテンプレート。

    ★「今の時刻」ではなく「そのタスクが担当する時刻」を渡す
      こうすることで、遅れて実行されても再試行されても
      書き込み先の列が必ず同じになり、CSVの列が二重に増えない。
    """
    return "{{ data_interval_start.in_timezone('Asia/Tokyo').isoformat() }}"
