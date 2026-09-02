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

★「1つでも取れていれば成功」をトリガールールで表している
  欠測スロットは学習時に自動で除外されるので、部分的な欠けは許容できる。
  全部そろわないと赤くする作りにすると、天気だけ落ちた日も全滅した日も
  同じ見え方になり、本当に困っている状態を見逃す。

  最後の collected タスクに one_success を付けることで、
    ・1つでも成功  → 実行は成功
    ・すべて失敗    → 実行は失敗(アラートにつながる)
  を、Python のコードを書かずに表現している。
  (PythonOperator を使わないので、共有ワーカーの環境にも依存しない)

★冪等性
  書き込む列名は「そのタスクが担当する時刻」をスロットに丸めたもの。
  再試行しても backfill しても同じ列を上書きするだけなので、
  CSVの列が二重に増えることはない。

必要な設定:
    dags/disney_settings.py の IMAGE を、push 済みイメージ名に書き換える。
    ★Airflow の画面(Admin → Variables)はさわりません。
    共有環境なので、設定は DAG と一緒に Git へ push するだけで完結します。
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

# ★Airflow 3 では標準オペレータの置き場所が変わった
#   実環境(Airflow 3.1.7)のログに、この警告が出ていた:
#     The `airflow.operators.empty.EmptyOperator` attribute is deprecated.
#     Please use `airflow.providers.standard.operators.empty.EmptyOperator`.
#   新しい場所を先に試し、無ければ従来の場所に落ちる。
#   こうしておけば Airflow 2 でも 3 でも動く。
try:
    from airflow.providers.standard.operators.empty import EmptyOperator
except ImportError:  # Airflow 2 系
    from airflow.operators.empty import EmptyOperator

from disney_common import (
    COLLECT_CPU,
    COLLECT_MEMORY,
    COLLECTOR_SCHEDULE,
    dag_kwargs,
    JST,
    make_task,
)

with DAG(
    dag_id="disney_collector",
    description="待ち時間(ランド/シー)と舞浜の天気を30分ごとに収集する",
    schedule=COLLECTOR_SCHEDULE,
    start_date=pendulum.datetime(2026, 1, 1, tz=JST),
    # 現在値を返すAPIなので、過去にさかのぼって取ることはできない
    catchup=False,
    # 同じCSVを同時に書かない
    max_active_runs=1,
    tags=["disney", "collect"],
    doc_md=__doc__,
    # 共有環境向けの指定(同時に使う枠の上限・優先度)
    **dag_kwargs(),
) as dag:
    # 収集は軽い(HTTPで取ってCSVに1列足すだけ)ので、資源は控えめでよい。
    # 30分スロットなので、次の実行までに終わる範囲で再試行する。
    common = dict(memory=COLLECT_MEMORY, cpu=COLLECT_CPU, retries=2,
                  retry_delay_minutes=3, timeout_minutes=10)

    tasks = [
        make_task("collect_tdl",
                  ["collect", "--park", "tdl"], **common),
        make_task("collect_tds",
                  ["collect", "--park", "tds"], **common),
        make_task("collect_weather",
                  ["collect", "--weather"], **common),
    ]

    # ★1つでも取れていれば、この実行は成功とみなす
    #   すべて失敗したときだけ upstream_failed になり、実行が赤くなる。
    collected = EmptyOperator(task_id="collected", trigger_rule="one_success")

    tasks >> collected


