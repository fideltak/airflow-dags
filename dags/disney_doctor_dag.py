# -*- coding: utf-8 -*-
"""
dags/disney_doctor_dag.py
=========================
うまく動かないときに、どこで詰まっているかを切り分けるための DAG。

★なぜ必要か
  収集や学習が失敗したとき、Airflow の画面に出るのは

      heartbeat timeout
      state mismatch ... finished with state failed
      failed

  という、原因の分からない形になりがち。これは
  「Pod が起動できず、5分間ハートビートが途絶えて強制終了された」
  ときの見え方で、本当の理由(イメージが無い / Secret が無い /
  S3 に届かない)は Kubernetes 側にしか残らない。

  この DAG は原因を上から順に切り分ける。
  **どこまで緑になったか**で、詰まっている場所が分かる。

      1_platform  … そもそも Pod を起動できるか
                    (公開イメージを使う。自作イメージの問題を排除する)
      2_image     … 自作イメージを取得して起動できるか
      3_secret    … S3 の鍵を Secret から渡せているか
      4_storage   … S3 に読み書きできるか
      5_data      … 収集データが貯まっているか

  例)
    1 で失敗   → Kubernetes の権限か資源の問題。管理者に相談
    2 で失敗   → イメージを push していないか、レジストリの認証
    3 で失敗   → Secret を作っていないか、名前が違う
    4 で失敗   → 鍵かエンドポイントかバケット名
    5 で失敗   → まだ収集できていない(異常ではないこともある)

使い方:
  Airflow の画面から手動で実行してください(定期実行はしません)。
"""

from __future__ import annotations

import os
import sys

# DAG と同じ場所にある disney_common を確実に読めるようにする
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pendulum
from airflow import DAG

from disney_common import (
    DATA_DIR,
    IMAGE,
    JST,
    MODEL_DIR,
    S3_BUCKET,
    S3_ENDPOINT,
    S3_PREFIX,
    S3_SECRET,
    dag_kwargs,
    make_task,
    setup_warnings,
)

# 切り分けの1段目で使う、どこにでもある小さなイメージ。
# ★自作イメージを使わないのが要点
#   ここが通れば「Kubernetes で Pod を起動する」仕組み自体は動いている、
#   と言い切れる。通らなければ自作イメージ以前の問題。
PROBE_IMAGE = os.getenv("DISNEY_PROBE_IMAGE", "busybox:1.36")

doc = __doc__ + f"""

## 現在の設定

| 項目 | 値 |
|---|---|
| イメージ | `{IMAGE}` |
| S3 エンドポイント | `{S3_ENDPOINT}` |
| バケット | `{S3_BUCKET}` |
| プレフィックス | `{S3_PREFIX}` |
| 鍵の Secret | `{S3_SECRET}` |
| データ | `{DATA_DIR}` |
| 成果物 | `{MODEL_DIR}` |

これらは `dags/disney_settings.py` に書いてあります。
Airflow の画面ではなく、そのファイルを直して Git へ push してください。
"""

for _w in setup_warnings():
    doc += f"\n> ⚠️ {_w}\n"

with DAG(
    dag_id="disney_doctor",
    description="うまく動かないときに、どこで詰まっているかを切り分ける",
    schedule=None,          # 手動でだけ実行する
    start_date=pendulum.datetime(2026, 1, 1, tz=JST),
    catchup=False,
    tags=["disney", "diagnose"],
    doc_md=doc,
    **dag_kwargs(),
) as dag:

    # --- 1. そもそも Pod を起動できるか -------------------------------
    # 自作イメージを使わないので、ここが通れば
    # 「KubernetesPodOperator で Pod を作る」仕組みは動いている。
    platform = make_task(
        "1_platform",
        ["この環境で Pod を起動できました"],
        image=PROBE_IMAGE,
        cmds=["echo"],
        memory="128Mi", cpu="100m",
        retries=0, timeout_minutes=5, startup_timeout_minutes=3,
        do_xcom_push=False,     # busybox には XCom を書く仕組みが無い
    )

    # --- 2. 自作イメージを取得して起動できるか ------------------------
    # ここで失敗するなら、イメージを push していないか、
    # レジストリの認証(imagePullSecrets)の問題。
    image_ok = make_task(
        "2_image",
        ["status"],
        memory="512Mi", cpu="500m",
        retries=0, timeout_minutes=5, startup_timeout_minutes=3,
        # status はモデルが無ければ 1 を返すが、ここでは
        # 「イメージが起動したか」だけを見たいので失敗にしない。
        skip_on_exit_code=1,
    )

    # --- 3. S3 の鍵を Secret から渡せているか -------------------------
    # 値そのものは出さない(ログに残ると鍵が漏れる)。
    # 入っているかどうかと、長さだけを確かめる。
    secret_ok = make_task(
        "3_secret",
        [
            'if [ -z "$AWS_ACCESS_KEY_ID" ] || [ -z "$AWS_SECRET_ACCESS_KEY" ]; then '
            '  echo "NG: S3の鍵が渡っていません"; '
            f'  echo "   Secret \'{S3_SECRET}\' がこの namespace にあるか確認してください"; '
            '  echo "   kubectl create secret generic ' + S3_SECRET + ' \\\\"; '
            '  echo "     --from-literal=AWS_ACCESS_KEY_ID=... \\\\"; '
            '  echo "     --from-literal=AWS_SECRET_ACCESS_KEY=..."; '
            '  exit 1; '
            'fi; '
            'echo "OK: 鍵が渡っています (ID=${#AWS_ACCESS_KEY_ID}文字 '
            'SECRET=${#AWS_SECRET_ACCESS_KEY}文字)"'
        ],
        cmds=["sh", "-c"],
        memory="512Mi", cpu="500m",
        retries=0, timeout_minutes=5, startup_timeout_minutes=3,
        do_xcom_push=False,
    )

    # --- 4. S3 に読み書きできるか -------------------------------------
    # 実際に小さなオブジェクトを置いて、読んで、消す。
    storage_ok = make_task(
        "4_storage",
        ["doctor", "--storage"],
        memory="512Mi", cpu="500m",
        retries=0, timeout_minutes=10, startup_timeout_minutes=3,
    )

    # --- 5. 収集データが貯まっているか --------------------------------
    # 足りなければスキップ(異常ではない)。
    data_ok = make_task(
        "5_data",
        ["check-data"],
        memory="2Gi", cpu="1",
        retries=0, timeout_minutes=15, startup_timeout_minutes=3,
        skip_on_exit_code=2,
    )

    platform >> image_ok >> secret_ok >> storage_ok >> data_ok
