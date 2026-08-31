# -*- coding: utf-8 -*-
"""
dags/disney_retrain_dag.py
==========================
モデルの継続学習DAG(毎日22時・閉園後)。

★このファイルは pandas も xgboost も import しない
  理由は disney_collector_dag.py と同じ。
  学習の実体は disney-jobs イメージの中にあり、この DAG は
  それを Pod として起動するだけ。

  ★これは共有環境では特に大事
    PCAI の Airflow は Executor のリソースが CPU 1 / メモリ 2Gi に
    固定されていて変更できない。学習を Airflow のワーカーで回すと
    その枠に縛られるうえ、他の利用者のタスクを圧迫する。
    別 Pod にすれば、必要なメモリを自分で指定できる。

★工程を分けている理由
      check_data → train → publish → verify → cleanup

  従来は「学習したら無条件に配信先へ上書き」だった。
  この作りだと、収集が一部失敗した日に学習して今より悪いモデルが
  出来ても、そのまま配信されてしまう。

  publish を独立させ、現行モデルより悪化していなければ差し替える。
  学習成果物は採用が決まるまで一時領域に置くので、
  学習が失敗しても推論サービスは動き続ける。

★モデルとエンコーダは必ず組で差し替える
  エンコーダはアトラクション名→IDの対応表で、学習のたびに変わりうる。
  モデルだけ新しくすると、IDの意味がずれて
  **エラーにならないまま予測だけが壊れる**。

必要な設定:
    dags/disney_settings.py の IMAGE を、push 済みイメージ名に書き換える。
    ★Airflow の画面(Admin → Variables)はさわりません。
"""

from __future__ import annotations

import os
import sys

# ★DAG と同じ場所にある disney_common を確実に読めるようにする
#   Airflow が sys.path に入れるのは DAG フォルダの**直下**まで。
#   git-sync がリポジトリを gitdags/dags/ のような入れ子で配置すると、
#   同じ階層に置いたモジュールでも import できず、
#     ModuleNotFoundError: No module named 'disney_common'
#   になる。自分の居場所を明示的に足しておけば、どこに置かれても動く。
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pendulum
from airflow import DAG

from disney_common import (
    COLLECT_CPU,
    COLLECT_MEMORY,
    JST,
    RETRAIN_SCHEDULE,
    TRAIN_CPU,
    TRAIN_MEMORY,
    dag_kwargs,
    make_task,
)

# disney.tasks が「まだやることが無い」ときに返す終了コード。
# これを受けたタスクは失敗ではなくスキップになる。
EXIT_SKIP = 2

with DAG(
    dag_id="disney_retrain",
    description="収集済みデータでXGBoostを再学習し、改善していれば配信モデルを差し替える",
    schedule=RETRAIN_SCHEDULE,
    start_date=pendulum.datetime(2026, 1, 1, tz=JST),
    catchup=False,
    # 学習の同時実行はモデルの取り合いになるので禁止
    max_active_runs=1,
    tags=["disney", "train"],
    doc_md=__doc__,
    **dag_kwargs(),
) as dag:

    # ★データが足りなければ、ここで後続ごとスキップする
    #   「まだ貯まっていない」のは異常ではない。
    #   失敗にすると本当の障害と区別がつかなくなるので、スキップにする。
    check_data = make_task(
        "check_data", ["check-data"],
        memory="2Gi", cpu="1", retries=1, timeout_minutes=20,
        skip_on_exit_code=EXIT_SKIP,
    )

    # 学習だけはメモリを多めに取る。
    # 成果物は一時領域(staging)へ書き、配信中のモデルには触らない。
    train = make_task(
        "train_model", ["train"],
        memory=TRAIN_MEMORY, cpu=TRAIN_CPU, retries=1, retry_delay_minutes=10,
        timeout_minutes=90, skip_on_exit_code=EXIT_SKIP,
    )

    # 採用判定。悪化していれば差し替えず、それでもタスクは成功にする
    # (「品質が足りないので現行を守った」は正常な結果のため)。
    publish = make_task(
        "publish_model", ["publish"],
        memory="2Gi", cpu="1", retries=0, timeout_minutes=20,
    )

    # 置けたことと、推論できることは別。実際に1件通して確かめる。
    verify = make_task(
        "verify_serving", ["verify"],
        memory="2Gi", cpu="1", retries=1, timeout_minutes=20,
    )

    # ★失敗しても必ず片付ける
    #   放っておくと共有ボリュームが一時ファイルで埋まる。
    cleanup = make_task(
        "cleanup", ["cleanup"],
        memory=COLLECT_MEMORY, cpu=COLLECT_CPU, retries=0, timeout_minutes=15,
        trigger_rule="all_done",
    )

    check_data >> train >> publish >> verify >> cleanup
