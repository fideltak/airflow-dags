# -*- coding: utf-8 -*-
"""
dags/disney_collector_dag.py
============================
待ち時間・天気の収集DAG(30分ごと・営業時間のみ)。

★このファイルは pandas も xgboost も import しない
  PCAI の Airflow は共有環境で、ワーカーのイメージを差し替えられない。
  DAG ファイルはスケジューラが繰り返し読み込むため、ここで重い依存を
  import すると、それが無い環境では**DAG が壊れて一覧にすら出てこない**。

  実際の収集処理は disney-jobs イメージの中にあり、
  この DAG はそれを Pod として起動するだけ。

★対象ごとにタスクを分けている理由
  ランド・シー・天気を1つにまとめると、シーだけ失敗したときに
  成功していたランドまでやり直すことになる。
  公式サイトへ余計な負荷をかけないためにも別々にする。

★冪等性
  書き込む列名は「そのタスクが担当する時刻」をスロットに丸めたもの。
  再試行しても backfill しても同じ列を上書きするだけなので、
  CSVの列が二重に増えることはない。

必要な設定(Airflow → Admin → Variables):
    disney_image          ジョブ用イメージ 例: registry.example.com/disney-jobs:2.0.0
    disney_shared_claim   共有PVC 既定: kubeflow-shared-pvc
"""

from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from disney_common import (
    COLLECTOR_SCHEDULE,
    DEFAULT_ARGS,
    JST,
    make_task,
    slot_template,
)

TARGETS = {
    "collect_tdl": "ランド",
    "collect_tds": "シー",
    "collect_weather": "天気",
}


def summarize(**context):
    """3つの収集結果をまとめて記録する。

    ★1つでも取れていれば成功にする
      欠測スロットは学習時に自動で除外されるので、部分的な欠けは許容できる。
      ここで「1つでも失敗したら赤」にすると、天気だけ落ちた日も
      全滅した日も同じ見え方になり、本当に困っている状態を見逃す。

      全滅したときだけ失敗させて、アラートにつなげる。
    """
    dag_run = context["dag_run"]
    slot = context["data_interval_start"].in_timezone("Asia/Tokyo")

    states = {}
    for task_id, label in TARGETS.items():
        ti = dag_run.get_task_instance(task_id)
        states[label] = ti.state if ti is not None else "unknown"

    print(f"=== スロット {slot:%Y/%m/%d %H:%M} の収集結果 ===")
    for label, state in states.items():
        mark = "✅" if state == "success" else "❌"
        print(f"  {mark} {label}: {state}")

    got = [k for k, v in states.items() if v == "success"]
    missing = [k for k, v in states.items() if v != "success"]

    if not got:
        raise RuntimeError(
            f"スロット {slot:%Y/%m/%d %H:%M}: すべての収集に失敗しました。"
            "ネットワーク(SSL/プロキシ)と公式サイトの状況を確認してください。"
        )
    if missing:
        print(f"⚠️ 取得できなかったもの: {', '.join(missing)} "
              f"(欠測スロットは学習時に自動で除外されます)")
    return {"slot": str(slot), "collected": got, "missing": missing}


with DAG(
    dag_id="disney_collector",
    description="待ち時間(ランド/シー)と舞浜の天気を30分ごとに収集する",
    schedule=COLLECTOR_SCHEDULE,
    start_date=pendulum.datetime(2026, 1, 1, tz=JST),
    # 現在値を返すAPIなので、過去にさかのぼって取ることはできない
    catchup=False,
    # 同じCSVを同時に書かない
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["disney", "collect"],
    doc_md=__doc__,
) as dag:

    slot = slot_template()

    # 収集は軽い(HTTPで取ってCSVに1列足すだけ)ので、資源は控えめでよい。
    # 30分スロットなので、次の実行までに終わる範囲で再試行する。
    common = dict(memory="512Mi", cpu="500m", retries=2,
                  retry_delay_minutes=3, timeout_minutes=10)

    tasks = [
        make_task("collect_tdl",
                  ["collect", "--park", "tdl", "--slot", slot], **common),
        make_task("collect_tds",
                  ["collect", "--park", "tds", "--slot", slot], **common),
        make_task("collect_weather",
                  ["collect", "--weather", "--slot", slot], **common),
    ]

    # ★一部が失敗しても必ず動くよう all_done にする
    #   ここは軽い処理なので、Pod を起こさず Airflow 側で実行する。
    summary = PythonOperator(
        task_id="summarize",
        python_callable=summarize,
        trigger_rule="all_done",
    )

    tasks >> summary
