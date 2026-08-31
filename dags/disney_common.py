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

★設定は dags/disney_settings.py に書く(Airflow の画面はさわらない)
  PCAI の Airflow は**複数人で共有**しているため、
  Admin → Variables を前提にした作りにはしない。

    ・Variables は全員で1つの名前空間を共有する(名前がぶつかる・
      そもそも権限が無いことが多い)
    ・DAG の先頭で Variable.get を呼ぶと、**DAG を読み直すたびに
      メタDBへ問い合わせ**が飛ぶ。Airflow は既定で30秒ごとに
      読み直すので、共有のスケジューラを圧迫する
      (Airflow 公式も best-practices で明確に禁じている)

  そのため、環境ごとに変わる値は同じ階層の disney_settings.py に置き、
  **DAG と一緒に Git へ push するだけで設定が終わる**ようにしている。
  管理者が Variable を設定してくれた場合は、それが優先されるが、
  その読み取りは Jinja テンプレート経由なので**実行時にしか起きない**。
"""

from __future__ import annotations

import datetime
import os
import sys

import pendulum

# 同じ階層の設定ファイルを読む(git-sync の入れ子配置でも読めるように)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import disney_settings as settings                       # noqa: E402

# ==========================================
# 基本設定
# ==========================================
JST = pendulum.timezone("Asia/Tokyo")

# ★収集・学習のスケジュールと営業時間
#   本来は disney/config.py が唯一の正だが、この DAG からは import できない
#   (共有 Airflow に依存が無いため)。そこで値をここに写している。
#   ずれると気づけないので、tests/check_dags.py が
#   config 側と一致することを検査している。
OPEN_HOUR = 9
CLOSE_HOUR = 21
SLOT_MINUTES = 30
RETRAIN_AT_HOUR = 22

COLLECTOR_SCHEDULE = "0,30 9-20 * * *"    # 9:00〜20:30 の30分毎
RETRAIN_SCHEDULE = "0 22 * * *"           # 毎日22時(閉園後)


def _overridable(var_name, value):
    """管理者が Variable を設定していればそれを使う、という指定を作る。

    ★ここが DB を叩かないための肝
      文字列をそのまま返さず Jinja テンプレートにしておくと、
      評価されるのは**タスクが動く瞬間**になる。
      DAG の読み込み時には一切 DB に触らないので、
      共有スケジューラに負荷をかけない。

      Variable が無ければ第2引数(設定ファイルの値)がそのまま使われる。
      つまり **Variable を作らなくても動く**。

    テンプレートが使えない場所(Secret 名など)では使わないこと。
    """
    safe = str(value).replace("'", "")
    return "{{ var.value.get('%s', '%s') }}" % (var_name, safe)


# ==========================================
# 保存先 (PCAI の Data Fabric オブジェクトストア)
# ==========================================
# ★収集CSVも学習成果物も S3 に置く
#   PVC を共有する必要がなくなるので、
#     ・ReadWriteMany に対応した StorageClass を用意しなくてよい
#     ・推論サービスを何レプリカにしても取り合いにならない
#   という利点がある。
S3_ENDPOINT = settings.get("S3_ENDPOINT")
S3_BUCKET = settings.get("S3_BUCKET")
S3_PREFIX = settings.get("S3_PREFIX")

DATA_DIR = f"s3://{S3_BUCKET}/{S3_PREFIX}/data"
MODEL_DIR = f"s3://{S3_BUCKET}/{S3_PREFIX}/artifacts"

# S3 の鍵を入れた Kubernetes Secret の名前。
# ★鍵を DAG に書かない
#   DAG は Git に置くので、鍵を書くと履歴に残って消せなくなる。
#   Secret から環境変数として渡す。
#     kubectl create secret generic disney-s3 \
#       --from-literal=AWS_ACCESS_KEY_ID=... \
#       --from-literal=AWS_SECRET_ACCESS_KEY=...
S3_SECRET = settings.get("S3_SECRET")

# ジョブ用イメージ
IMAGE = settings.get("IMAGE")
IMAGE_PULL_POLICY = settings.get("IMAGE_PULL_POLICY")
IMAGE_PULL_SECRET = settings.get("IMAGE_PULL_SECRET")
NAMESPACE = settings.get("NAMESPACE")    # 空なら Pod と同じ namespace

# 共有環境への配慮(詳細は disney_settings.py)
MAX_ACTIVE_TASKS = int(settings.get("MAX_ACTIVE_TASKS") or 3)
PRIORITY_WEIGHT = int(settings.get("PRIORITY_WEIGHT") or 0)
POOL = settings.get("POOL")

DEFAULT_ARGS = {
    "owner": "disney",
    "depends_on_past": False,
    # ★共有ワーカーの枠を奪わない
    #   このDAGは多少遅れても困らないので、優先度を下げておく。
    "priority_weight": PRIORITY_WEIGHT,
}
if POOL:
    DEFAULT_ARGS["pool"] = POOL


def make_task(task_id, arguments, memory="2Gi", cpu="1",
              retries=1, retry_delay_minutes=5, timeout_minutes=30,
              startup_timeout_minutes=4, skip_on_exit_code=None, **kwargs):
    """タスク1つ分の KubernetesPodOperator を組み立てる。

    ★なぜ KubernetesPodOperator なのか
      共有 Airflow のワーカーには学習用の依存が無く、
      Executor のリソースも CPU 1 / メモリ 2Gi に固定されていて変えられない。
      別 Pod として起動すれば、必要な依存も必要なメモリも自分で決められる。

    Args:
        arguments: disney.tasks に渡す引数
                   例) ["collect", "--park", "tdl", "--slot", "{{ ... }}"]
        memory/cpu: この処理に必要な量。学習は多めに要る。
        startup_timeout_minutes: Pod が起動するまで待つ上限。
        skip_on_exit_code: この終了コードなら「スキップ」として扱う。
                           disney.tasks は 2 を「まだやることが無い」の意味で返す。
    """
    from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
    from kubernetes.client import models as k8s

    env = {
        "DISNEY_DATA_DIR": DATA_DIR,
        "DISNEY_MODEL_DIR": MODEL_DIR,
        "S3_ENDPOINT_URL": _overridable("disney_s3_endpoint", S3_ENDPOINT),
        "TZ": "Asia/Tokyo",
        # 作業ディレクトリを実行ごとに分けるのに使う
        "AIRFLOW_RUN_ID": "{{ run_id }}",
    }

    env_vars = [k8s.V1EnvVar(name=k, value=v) for k, v in env.items()]

    # ★鍵は Secret から取り込む(DAG にも Git にも書かない)
    if S3_SECRET:
        for key in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
            env_vars.append(k8s.V1EnvVar(
                name=key,
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name=S3_SECRET, key=key,
                        # ★Secret が無くても Pod は起動させる
                        #   optional を付けないと、Secret が存在しないとき
                        #   kubelet はコンテナを起動せずに再試行し続ける
                        #   (Kubernetes 公式: "None of a Pod's containers
                        #    will start until all non-optional Secrets are
                        #    available")。Airflow からはハートビートが
                        #   途切れただけに見え、原因が分からない。
                        #   起動さえすれば、中の doctor/collect が
                        #   「鍵が渡っていません」と理由を出して終われる。
                        optional=True)),
            ))

    params = dict(
        task_id=task_id,
        name=f"disney-{task_id}".replace("_", "-"),
        # 管理者が Variable を設定していればそちらを使う(任意)
        image=_overridable("disney_image", IMAGE),
        image_pull_policy=IMAGE_PULL_POLICY,
        arguments=list(arguments),
        env_vars=env_vars,
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": cpu, "memory": memory},
            limits={"cpu": cpu, "memory": memory},
        ),
        # 実行のたびに Pod を作り直し、終わったら片付ける
        is_delete_operator_pod=True,
        # ★Pod を起動した Airflow 側から見えるように、中のログを引き上げる
        get_logs=True,
        # ★Pod が起動できなかったときに、Kubernetes のイベントを残す
        #   「なぜ起動しなかったのか」(ImagePullBackOff / Secret が無い /
        #   資源が足りない)は Pod のログではなくイベントに出るため、
        #   これが無いと原因が分からないまま失敗だけが残る。
        log_events_on_failure=True,
        # ★起動を待つ上限を明示する
        #   既定のままだと、イメージを取得できないときに
        #   Airflow のハートビート上限(300秒)より先に諦められず、
        #   「heartbeat timeout → state mismatch → failed」という
        #   原因の分からない形で落ちる。
        #   ここを 300秒より短くしておけば、
        #   「Pod took too long to start」と明示されて理由が追える。
        startup_timeout_seconds=int(startup_timeout_minutes * 60),
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


def dag_kwargs():
    """全DAG共通の、共有環境で行儀よくするための指定。"""
    return {
        # ★他の人のタスクを待たせないよう、同時に使う枠に上限を設ける
        "max_active_tasks": MAX_ACTIVE_TASKS,
        "default_args": DEFAULT_ARGS,
    }


def setup_warnings():
    """このままでは動かない設定を挙げる。空リストなら大丈夫。

    ★一番多い失敗がイメージの指定漏れ
      レジストリ名の無いイメージ名だと Kubernetes は docker.io から
      取りに行き、ImagePullBackOff で Pod が起動しない。その結果
      ハートビートが途切れ、「heartbeat timeout」という
      原因の分かりにくい形で失敗する。
    """
    return settings.needs_attention()


def image_warning():
    """イメージの指定が本番で通らない形なら、その理由を返す。無ければ None。"""
    for msg in setup_warnings():
        if "IMAGE" in msg or "イメージ" in msg or "レジストリ" in msg:
            return msg
    return None


def slot_template():
    """収集スロットとして渡すテンプレート。

    ★「今の時刻」ではなく「そのタスクが担当する時刻」を渡す
      こうすることで、遅れて実行されても再試行されても
      書き込み先の列が必ず同じになり、CSVの列が二重に増えない。
    """
    return "{{ data_interval_start.in_timezone('Asia/Tokyo').isoformat() }}"
