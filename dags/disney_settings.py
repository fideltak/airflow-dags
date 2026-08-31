# -*- coding: utf-8 -*-
"""
dags/disney_settings.py
=======================
★ここだけ書き換えれば動きます。Airflow の画面は一切さわりません。

なぜこのファイルがあるのか
--------------------------
PCAI の Airflow は**複数人で共有**しています。共有環境では、

  ・Admin → Variables は全員で1つの名前空間を共有する
    (他チームと名前がぶつかる。そもそも権限が無いことも多い)
  ・Variable を DAG の先頭で読むと、**DAG が読み込まれるたびに
    メタDBへ問い合わせ**が飛ぶ。Airflow は既定で30秒ごとに
    全DAGを読み直すので、共有のスケジューラを地味に圧迫する
    (Airflow 公式も「top-level code で Variable.get を使うな」と明記)

そこで、環境ごとに変わる値はこのファイルに置き、
**DAG と一緒に Git へ push するだけ**で設定が終わるようにしています。
このファイルの読み取りはただの Python なので、DBには一切触れません。

値の決まり方(上が強い)
----------------------
  1. 環境変数          … 手元で試すとき用
  2. このファイルの値  ← 通常はここを書き換える
  3. Airflow Variable  … 管理者が設定した場合のみ。実行時にだけ読むので
                          共有スケジューラに負荷をかけません(任意)

  ※ 3 は「設定されていれば使う」という**任意の上書き**です。
    設定しなくても 2 の値で動きます。
"""

from __future__ import annotations

import os

# ==========================================================
# 1. ジョブ用イメージ  ★まずここを直してください
# ==========================================================
# ビルドした disney-jobs イメージを、PCAI から取得できる
# レジストリに push して、その名前をここに書きます。
#
#   ★レジストリ名を省くと動きません
#     "disney-jobs:2.0.0" と書くと Kubernetes は docker.io を
#     見に行き、ImagePullBackOff で Pod が起動しません。その結果
#     Airflow 側には「heartbeat timeout」としか出ず、原因が分からない
#     形で失敗します。必ず "レジストリ名/イメージ名:タグ" で書きます。
#
#   例) "registry.example.com/disney/disney-jobs:2.0.0"
IMAGE = "CHANGE-ME.registry.example.com/disney-jobs:2.0.0"

# イメージの取得方針。タグを固定して使うので、毎回取り直す必要はない。
IMAGE_PULL_POLICY = "IfNotPresent"

# レジストリに認証が要る場合の Secret 名。不要なら "" にする。
IMAGE_PULL_SECRET = "imagepull"

# ==========================================================
# 2. 保存先 (HPE Ezmeral Data Fabric オブジェクトストア)
# ==========================================================
S3_ENDPOINT = ("http://ext-datafabric01-s3-service.ezdata-system"
               ".svc.cluster.local:30000")

# ★バケットは先に作っておく必要があります
#   Data Fabric では、既定アカウント以外のバケットを SDK から
#   作れません。Object Store の画面か mc コマンドで作ってください。
S3_BUCKET = "bucket-ozawa-ryota"
S3_PREFIX = "Airflow"

# S3 の鍵を入れた Kubernetes Secret の名前。
#   ★鍵そのものはここに書きません(Git の履歴に残ると消せません)
#     kubectl create secret generic disney-s3 \
#       --from-literal=AWS_ACCESS_KEY_ID=... \
#       --from-literal=AWS_SECRET_ACCESS_KEY=...
S3_SECRET = "disney-s3"

# ==========================================================
# 3. 実行する場所
# ==========================================================
# Pod を作る namespace。空にすると Airflow と同じ namespace になる。
# 共有環境では、自分の namespace が決まっていればそこを書く。
NAMESPACE = ""

# ==========================================================
# 4. 共有環境への配慮
# ==========================================================
# ★同時に使うタスク枠の上限
#   Airflow のワーカー枠は全員で共有しています。収集は3つ同時に
#   動きますが、それ以上は増やさないことで、他の人のタスクを
#   待たせないようにします。
MAX_ACTIVE_TASKS = 3

# ★優先度
#   このDAGは多少遅れても困りません。負の値にしておくと、
#   枠を取り合ったときに他の人のタスクが先に流れます。
PRIORITY_WEIGHT = -10

# ★使用するプール
#   専用プールを作ってもらえた場合はその名前を書く。
#   空なら既定のプール(default_pool)を使う。
POOL = ""

# ==========================================================
# 以下は書き換え不要
# ==========================================================


def get(name):
    """設定値を取り出す。環境変数があればそれを優先する。

    環境変数は手元で試すとき用。`DISNEY_IMAGE=... python ...` のように
    書けば、このファイルを書き換えずに差し替えられる。
    """
    env = os.getenv(f"DISNEY_{name}")
    if env is not None and env != "":
        return env
    return globals().get(name)


def needs_attention():
    """このままでは動かない設定を挙げる。空リストなら大丈夫。

    ★DAG の読み込み時に例外を投げない
      ここで落とすと DAG が一覧から消えてしまい、画面で原因を
      確認することすらできなくなる。あくまで「知らせる」だけにする。
    """
    problems = []

    image = str(get("IMAGE") or "")
    if "CHANGE-ME" in image:
        problems.append(
            "IMAGE がひな形のままです。dags/disney_settings.py の IMAGE を、"
            "push 済みイメージの名前に書き換えてください。")
    else:
        head = image.split("@")[0].split("/")[0] if "/" in image else ""
        if not ("." in head or ":" in head or head == "localhost"):
            problems.append(
                f"IMAGE '{image}' にレジストリ名がありません。このままでは "
                f"docker.io を見に行き ImagePullBackOff になります。"
                f"'レジストリ名/イメージ名:タグ' の形で書いてください。")

    if not str(get("S3_BUCKET") or ""):
        problems.append("S3_BUCKET が空です。")
    if not str(get("S3_ENDPOINT") or "").startswith("http"):
        problems.append("S3_ENDPOINT が URL になっていません。")

    return problems
