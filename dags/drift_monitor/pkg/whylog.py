import json
import subprocess
import sys


def check_whylogs():
    try:
        import whylogs
    except ImportError:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "whylogs"])


def check_data_drift(train_df, trace_df):
    check_whylogs()
    import whylogs as why
    from whylogs.viz.drift.column_drift_algorithms import calculate_drift_scores

    train_profile = why.log(pandas=train_df).profile()
    trace_profile = why.log(pandas=trace_df).profile()

    train_view = train_profile.view()
    trace_view = trace_profile.view()
    scores = calculate_drift_scores(
        target_view=trace_view,  # data to check drift on
        reference_view=train_view,  # baseline data
        with_thresholds=True  # include thresholds & categories
    )
    drift_metrics = []
    for col, info in scores.items():
        if info is None:
            print(f"{col}: no distribution metrics (skipped)")
            continue
        alg = info["algorithm"]
        stat = info["statistic"]
        pval = info["pvalue"]
        cat = info["drift_category"]
        drift_metrics.append({
            'name': f"ks_value_{col}",
            'value': f"{stat:.4f}"
        })
        drift_metrics.append({
            'name': f"p_value_{col}",
            'value': f"{pval:.4f}"
        })

    return drift_metrics


def convert_tuples(obj):
    if isinstance(obj, dict):
        return {k: convert_tuples(v) for k, v in obj.items()}
    elif isinstance(obj, tuple):
        return list(obj)
    elif isinstance(obj, list):
        return [convert_tuples(i) for i in obj]
    else:
        return obj
