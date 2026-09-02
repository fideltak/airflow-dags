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

  ★Variable による上書きも入れていない
    当初は「設定されていれば優先する」という任意の上書きを
    Jinja テンプレート経由で入れていたが、Airflow 3 では
    未定義のキーに触れた時点で例外になり、**タスクが必ず失敗する**
    ことが分かったため取りやめた(実環境で発生・再現確認済み)。
    設定は disney_settings.py が唯一の正。
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
    """設定ファイルの値をそのまま返す。

    ★Jinja テンプレートで Variable を参照するのはやめた
      当初は「管理者が Variable を設定していればそちらを使う」という
      任意の上書きを入れていた。しかし Airflow 3 では、未定義のキーに
      触れた時点で **例外**になることが分かった(実環境で発生)。

          AirflowRuntimeError: VARIABLE_NOT_FOUND:
            {'message': 'Variable disney_image not found'}
          Exception rendering Jinja template for task ..., field 'image'.

      Jinja の `default` フィルタも `var.value.get(key, default)` も、
      値が返る前に例外が飛ぶので**役に立たない**。
      (Airflow 3.1.7 の公式イメージで3通り試して確認済み)

      Variable による上書きは「あれば便利」程度のものだったのに対し、
      これが原因で**タスクが必ず失敗する**のでは割に合わない。
      設定は dags/disney_settings.py にあり Git で管理できるので、
      Variable に頼る理由はもともと無い。

    ★結果として、こちらのほうが良くなった
      ・DAG の読み込み時も実行時も、メタDBに一切触らない
      ・共有環境で Variable の名前がぶつからない
      ・値がそのまま入るので、UI の Rendered Template で読める

    Args:
        var_name: 以前 Variable 名だったもの。いまは記録用。
        value: 設定ファイルの値。
    """
    return str(value)


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

# 手動で作った AWS 互換キーを持つ Secret(通常は不要)。
# ★PCAI と Ezmeral が接続済みなら空のままで OK
#   PCAI が /etc/secrets/ezua/.auth_token を自動で配るため、
#   手動の Secret は要りません。storage.py がそのトークンを使います。
S3_SECRET = settings.get("S3_SECRET")

# PCAI が配っている Ezmeral 認証トークンの Secret 名。
# ★これを Pod にボリュームマウントすることで、
#   /etc/secrets/ezua/.auth_token から自動でトークンを読めるようになる。
EZUA_TOKEN_SECRET = settings.get("EZUA_TOKEN_SECRET")

# ジョブ用イメージ
IMAGE = settings.get("IMAGE")
IMAGE_PULL_POLICY = settings.get("IMAGE_PULL_POLICY")
IMAGE_PULL_SECRET = settings.get("IMAGE_PULL_SECRET")
NAMESPACE = settings.get("NAMESPACE")    # 空なら Pod と同じ namespace

# 共有環境への配慮(詳細は disney_settings.py)
MAX_ACTIVE_TASKS = int(settings.get("MAX_ACTIVE_TASKS") or 3)
PRIORITY_WEIGHT = int(settings.get("PRIORITY_WEIGHT") or 0)
POOL = settings.get("POOL")

# Pod に割り当てる資源(ResourceQuota に合わせて settings で下げられる)
COLLECT_MEMORY = settings.get("COLLECT_MEMORY")
COLLECT_CPU = settings.get("COLLECT_CPU")
TRAIN_MEMORY = settings.get("TRAIN_MEMORY")
TRAIN_CPU = settings.get("TRAIN_CPU")

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
              startup_timeout_minutes=4, skip_on_exit_code=None,
              env_vars_extra=None, **kwargs):
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
    if env_vars_extra:
        env.update({k: str(v) for k, v in env_vars_extra.items()})

    env_vars = [k8s.V1EnvVar(name=k, value=v) for k, v in env.items()]

    # ★手動キー Secret がある場合はそこから env として渡す(代替手段)
    #   通常は EZUA_TOKEN_SECRET のボリュームマウントを使うので不要。
    if S3_SECRET:
        for key in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
            env_vars.append(k8s.V1EnvVar(
                name=key,
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name=S3_SECRET, key=key,
                        optional=True)),
            ))

    # ★PCAI が配っている Ezmeral 認証トークンをボリュームマウントする
    #   PCAI と Ezmeral Data Fabric が接続済みなら、ユーザーの namespace に
    #   'access-token' という Secret が自動作成されている。
    #   それを /etc/secrets/ezua/.auth_token としてマウントすれば、
    #   storage.py が自動でそのトークンを使い、S3 への読み書きができる。
    #   (アクセスキーを手動で作る必要はない)
    #
    #   optional=True にすることで、Secret が見つからなくても Pod は起動する。
    #   鍵が無ければ 3_secret / 4_storage の段で理由が表示される。
    volumes = []
    volume_mounts = []
    if EZUA_TOKEN_SECRET:
        volumes.append(k8s.V1Volume(
            name="ezua-token",
            secret=k8s.V1SecretVolumeSource(
                secret_name=EZUA_TOKEN_SECRET,
                items=[k8s.V1KeyToPath(key="AUTH_TOKEN", path=".auth_token")],
                optional=True,
            ),
        ))
        volume_mounts.append(k8s.V1VolumeMount(
            name="ezua-token",
            mount_path="/etc/secrets/ezua",
            read_only=True,
        ))

    params = dict(
        task_id=task_id,
        name=f"disney-{task_id}".replace("_", "-"),
        # 設定ファイル(disney_settings.py)の値をそのまま使う
        image=_overridable("disney_image", IMAGE),
        image_pull_policy=IMAGE_PULL_POLICY,
        arguments=list(arguments),
        env_vars=env_vars,
        volumes=volumes,
        volume_mounts=volume_mounts,
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": cpu, "memory": memory},
            limits={"cpu": cpu, "memory": memory},
        ),
        # 実行のたびに Pod を作り直し、終わったら片付ける
        #   ★`is_delete_operator_pod` は非推奨(検証で判明)
        #     `on_finish_action` が後継。delete_pod は
        #     「成功でも失敗でも消す」で、従来の True と同じ意味。
        #     残しておくと共有クラスタに Pod がたまる。
        on_finish_action="delete_pod",
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

    # ★設定がひな形のままなら、Pod を作らずにここで止める
    #   そのまま起動すると、Kubernetes が存在しないレジストリを
    #   引きに行き、ImagePullBackOff を数分繰り返してから失敗する。
    #   実際に "CHANGE-ME.registry.example.com" を引きに行って
    #   「no such host」で落ちた。時間の無駄だし、ログも読みにくい。
    #   ここで止めれば、1行で理由が分かる。
    op = KubernetesPodOperator(**params)
    return _guard_placeholder(op)


class _StopIfNotConfigured:
    """設定が未完了なら、Pod を作る前に理由を出して失敗させる。

    ★execute を包む
      DAG の読み込み時に例外を投げてはいけない(一覧から消えてしまい、
      画面で原因を確かめられなくなる)。実行の瞬間に止めるのが安全。
    """

    def __init__(self, run):
        self._run = run

    def __call__(self, context):
        problems = settings.needs_attention()
        if problems:
            from airflow.exceptions import AirflowFailException

            lines = ["設定が未完了のため、Pod を起動せずに止めました。"]
            lines += [f"  ・{p}" for p in problems]
            lines.append("")
            lines.append("  直したら、DAG と一緒に Git へ push してください。")
            lines.append("  (Airflow の画面での操作は要りません)")
            msg = "\n".join(lines)
            print(msg)
            # ★再試行しない
            #   設定を直さないかぎり何度やっても同じなので、
            #   AirflowFailException で即座に確定させる。
            raise AirflowFailException(msg)
        return self._run(context)


def _guard_placeholder(op):
    """オペレータの execute を、設定チェック付きに差し替える。"""
    op.execute = _StopIfNotConfigured(op.execute)
    return op


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


# ==========================================
# 共有環境での自己診断
# ==========================================
# ★ここだけは Pod を作らずに、Airflow のワーカーの中で直接動く
#   「Pod が起動できない」のか「そもそもワーカーが動いていない」のかは、
#   Pod を作る方法では区別できない。どちらも同じ heartbeat timeout に
#   見えてしまう。ワーカーの中で動く処理を1つ用意しておけば、
#   そこが通るかどうかで責任の所在がはっきりする。
#
#   標準ライブラリしか使わない。共有ワーカーに何が入っているか
#   分からないため。

_SA = "/var/run/secrets/kubernetes.io/serviceaccount"

# Kubernetes の量の書き方 (6Gi / 512Mi / 2G …) を数値にするための表
_UNITS = {"": 1, "k": 10 ** 3, "M": 10 ** 6, "G": 10 ** 9, "T": 10 ** 12,
          "Ki": 2 ** 10, "Mi": 2 ** 20, "Gi": 2 ** 30, "Ti": 2 ** 40}


def _bytes(text):
    """"6Gi" のような書き方をバイト数にする。読めなければ None。"""
    if text is None:
        return None
    t = str(text).strip()
    for unit in sorted(_UNITS, key=len, reverse=True):
        if unit and t.endswith(unit):
            try:
                return int(float(t[:-len(unit)]) * _UNITS[unit])
            except ValueError:
                return None
    try:
        return int(float(t))
    except ValueError:
        return None


def _human(n):
    """バイト数を読みやすい形にする。"""
    if n is None:
        return "?"
    for unit in ("Ti", "Gi", "Mi", "Ki"):
        if n >= _UNITS[unit]:
            return f"{n / _UNITS[unit]:.1f}{unit}"
    return f"{n}B"


def _read(path):
    try:
        with open(path, encoding="utf-8") as f:
            return f.read().strip()
    except Exception:
        return None


def _k8s_api(path, method="GET", body=None, timeout=10):
    """Pod に配られたトークンで Kubernetes API を叩く(標準ライブラリのみ)。"""
    import json
    import ssl
    import urllib.error
    import urllib.request

    token = _read(f"{_SA}/token")
    if not token:
        raise RuntimeError(
            "ServiceAccount のトークンがありません。"
            "このプロセスは Kubernetes の Pod の中で動いていないようです。")

    host = os.getenv("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
    port = os.getenv("KUBERNETES_SERVICE_PORT", "443")
    url = f"https://{host}:{port}{path}"

    ctx = ssl.create_default_context(cafile=f"{_SA}/ca.crt")
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {token}")
    if data:
        req.add_header("Content-Type", "application/json")

    with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
        return json.loads(r.read().decode())


def _may_i(namespace, verb, resource, group=""):
    """自分がその操作をしてよいかを Kubernetes 自身に尋ねる。

    SelfSubjectAccessReview は、ログインできる全員に既定で許可されている
    (ClusterRole `system:basic-user`)ので、権限が無い環境でも尋ねられる。
    """
    res = _k8s_api(
        "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews",
        method="POST",
        body={"kind": "SelfSubjectAccessReview",
              "apiVersion": "authorization.k8s.io/v1",
              "spec": {"resourceAttributes": {
                  "namespace": namespace, "verb": verb,
                  "resource": resource, "group": group}}},
    )
    st = res.get("status", {})
    return bool(st.get("allowed")), st.get("reason") or ""


def worker_selftest():
    """Airflow のワーカーの中で、Pod を作らずに確かめられることを全部見る。

    ★これが動けば、少なくともワーカーは起きている
      失敗するのが**このタスクだけ**なら Airflow 基盤の問題。
      これが通って次(Pod を作る段)で失敗するなら、権限か資源の問題。

    例外は投げない。分かったことを全部出してから、
    致命的なものがあったときだけ最後に失敗させる。
    """
    lines = []
    fatal = []

    def say(s=""):
        lines.append(s)

    say("=" * 60)
    say("0. Airflow ワーカーの中から見た状態")
    say("=" * 60)
    say("★このタスクは新しい Pod を作りません。")
    say("  ここまで来ている時点で、ワーカーは正常に起動しています。")
    say("")

    # --- 実行環境 ---
    import getpass
    import platform
    import socket

    say(f"   ホスト名     : {socket.gethostname()}")
    say(f"   Python       : {platform.python_version()}")
    try:
        say(f"   実行ユーザー : {getpass.getuser()}")
    except Exception:
        pass

    ns = _read(f"{_SA}/namespace")
    say(f"   namespace    : {ns or '(取得できません)'}")
    say(f"   設定の場所   : {os.path.dirname(os.path.abspath(__file__))}")
    say("")

    # --- 設定の中身 ---
    #   ★表示する値は、実際に使われる値と同じ経路で取り出す
    #     ここで別の変数を見ていると、「表示は正しいのに動かない」
    #     という一番たちの悪い状態になる。
    say("-" * 60)
    say("いま使われている設定 (dags/disney_settings.py)")
    say("-" * 60)
    live = {k: settings.get(k) for k in
            ("IMAGE", "S3_ENDPOINT", "S3_BUCKET", "S3_PREFIX",
             "EZUA_TOKEN_SECRET", "NAMESPACE", "TRAIN_MEMORY", "TRAIN_CPU")}
    say(f"   イメージ             : {live['IMAGE']}")
    say(f"   S3                   : {live['S3_ENDPOINT']}")
    say(f"   バケット             : {live['S3_BUCKET']} / {live['S3_PREFIX']}")
    say(f"   Ezmeral トークン Secret : {live['EZUA_TOKEN_SECRET'] or '(未設定)'}")
    say(f"   namespace            : {live['NAMESPACE'] or '(Airflow と同じ)'}")
    say("")

    # ★Airflow Variable は見ない
    #   以前は「設定されていれば上書きする」作りにしていたが、
    #   Airflow 3 では未定義キーで例外になるため取りやめた。
    #   設定は dags/disney_settings.py が唯一の正になっている。
    say("   設定はすべて dags/disney_settings.py の値です")
    say("   (Airflow の Variables は参照していません)")
    say("")

    warns = setup_warnings()
    for w in warns:
        say(f"   ⚠️  {w}")
        fatal.append(w)
    if not warns:
        say("   ✅ 設定の書き方に問題はありません")
    say("")

    if not ns:
        say("   ⏭️  Kubernetes の中ではないため、以降は省略します")
        out = "\n".join(lines)
        print(out)
        if fatal:
            raise RuntimeError(" / ".join(fatal))
        return out

    target_ns = NAMESPACE or ns

    # --- Kubernetes API に届くか ---
    say("-" * 60)
    say("Kubernetes への問い合わせ")
    say("-" * 60)
    try:
        _k8s_api("/version")
        say("   ✅ Kubernetes API に届いています")
    except Exception as e:
        say(f"   ❌ Kubernetes API に届きません: {e.__class__.__name__}: {e}")
        say("      KubernetesPodOperator は使えません。管理者にご相談ください。")
        fatal.append("Kubernetes API に届きません")
        print("\n".join(lines))
        raise RuntimeError(" / ".join(fatal))

    # --- ★Pod を作ってよいか(RBAC) ---
    #   KubernetesPodOperator はワーカーの ServiceAccount で Pod を作る。
    #   共有 Airflow では、この権限が無いことがある。
    #   権限が無いと Pod が作られないまま時間だけが過ぎ、
    #   heartbeat timeout に化ける。
    say("")
    say("-" * 60)
    say(f"★Pod を作る権限 (namespace: {target_ns})")
    say("-" * 60)
    need = [("create", "pods", "", True),
            ("get", "pods", "", True),
            ("delete", "pods", "", True),
            ("get", "pods/log", "", True),
            ("list", "events", "", False)]
    for verb, resource, group, required in need:
        try:
            allowed, reason = _may_i(target_ns, verb, resource, group)
        except Exception as e:
            say(f"   ?  {verb:7} {resource:12} 確認できません ({e})")
            continue
        mark = "✅" if allowed else ("❌" if required else "⚠️ ")
        say(f"   {mark} {verb:7} {resource:12} "
            f"{'できます' if allowed else 'できません'} {reason}")
        if required and not allowed:
            fatal.append(f"{target_ns} で pod を {verb} する権限がありません")

    if fatal:
        say("")
        say("   ★これが原因です。")
        say("     KubernetesPodOperator はワーカーの ServiceAccount で")
        say("     Pod を作ります。その権限が無いと Pod は作られず、")
        say("     Airflow には heartbeat timeout としか出ません。")
        say("     管理者に、この namespace で pods を作る権限を")
        say("     依頼してください。")

    # --- 資源の空き ---
    say("")
    say("-" * 60)
    say("資源の空き (ResourceQuota)")
    say("-" * 60)
    try:
        q = _k8s_api(f"/api/v1/namespaces/{target_ns}/resourcequotas")
        items = q.get("items", [])
        if not items:
            say("   上限は設定されていません")
        for it in items:
            st = it.get("status", {})
            hard, used = st.get("hard", {}), st.get("used", {})
            say(f"   [{it['metadata']['name']}]")
            for k in sorted(hard):
                say(f"      {k:28} 使用 {used.get(k, '?')} / 上限 {hard[k]}")

            # ★学習ぶんが入るかを実際に計算する
            #   入らないと Pod は作られず、また heartbeat timeout に化ける。
            for key in ("limits.memory", "requests.memory"):
                if key not in hard:
                    continue
                free = _bytes(hard[key]) - _bytes(used.get(key, "0"))
                want = _bytes(live["TRAIN_MEMORY"])
                if free is None or want is None:
                    continue
                if want > free:
                    msg = (f"学習が要求する {live['TRAIN_MEMORY']} は "
                           f"空き({_human(free)})に入りません。"
                           f"dags/disney_settings.py の TRAIN_MEMORY を"
                           f"下げてください。")
                    say(f"      ❌ {msg}")
                    fatal.append(msg)
                else:
                    say(f"      ✅ 学習の {live['TRAIN_MEMORY']} は"
                        f"空き({_human(free)})に収まります")
                break
    except Exception as e:
        say(f"   確認できません({e.__class__.__name__}) — 権限が無いだけの場合もあります")

    say("")
    say("=" * 60)
    if fatal:
        say("結果: ❌ 先に進めない問題があります(上記)")
    else:
        say("結果: ✅ ワーカー側に問題はありません。次の段へ進みます")
    say("=" * 60)

    out = "\n".join(lines)
    print(out)
    if fatal:
        raise RuntimeError(" / ".join(fatal))
    return out
