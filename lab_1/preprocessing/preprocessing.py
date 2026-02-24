import ast
import json

import pandas as pd
import numpy as np


def extract_dict_features(x):
    if pd.isna(x):
        return pd.Series([0,0])
    d = ast.literal_eval(x)
    return pd.Series([d.get("cpus", 0), d.get("memory", 0)])


def cpu_stats(x):
    arr = np.fromstring(x.strip("[]"), sep=" ")
    if len(arr) == 0:
        return pd.Series([0., 0., 0.])
    return pd.Series([
        arr.mean(),
        arr.max(),
        arr.std()
    ])


def process_df(df: pd.DataFrame, medians: dict, train: bool = False):
    id_info = {
        "instance_index": int(df["instance_index"].iloc[0]),
        "cluster": str(df["cluster"].iloc[0]),
        "user": str(df["user"].iloc[0]),
        "machine_id": str(df["machine_id"].iloc[0]),
    }

    df = df.drop(
        [
            "Unnamed: 0",
            "collection_id",
            "alloc_collection_id",
            "collection_name",
            "collection_logical_name",
            "user",
            "instance_index",
            "start_after_collection_ids",
            "machine_id",
        ],
        axis=1
    )

    if train:
        df = df.sort_values("time")
        df = df.drop(["instance_events_type", "collections_events_type", "event"], axis=1)
    
    df[["resource_request_cpu", "resource_request_memory"]] = df["resource_request"].apply(extract_dict_features)
    df[["avg_usage_cpu", "avg_usage_memory"]] = df["average_usage"].apply(extract_dict_features)
    df[["max_usage_cpu", "max_usage_memory"]] = df["maximum_usage"].apply(extract_dict_features)
    df[["cpu_mean", "cpu_max", "cpu_std"]] = df["cpu_usage_distribution"].apply(cpu_stats)

    df = df.fillna(medians)

    df["duration"] = df["end_time"] - df["start_time"]
    df["time"] = df["time"] / 1e6
    df["duration"] = df["duration"] / 1e6

    df = df.drop(
        [
            "resource_request",
            "average_usage",
            "maximum_usage",
            "random_sample_usage",
            "cpu_usage_distribution",
            "tail_cpu_usage_distribution",
            "start_time",
            "end_time",
            "constraint",
            "cluster",
            "collection_type",
            "sample_rate",
            "scheduler",
            "scheduling_class",
        ],
        axis=1
    )
    return df, id_info
