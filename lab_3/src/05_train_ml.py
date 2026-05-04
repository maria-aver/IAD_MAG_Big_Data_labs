import os
import mlflow
import mlflow.sklearn
import polars as pl

from deltalake import DeltaTable

from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    mean_absolute_error,
    mean_squared_error,
    r2_score,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

from config import GOLD_FEATURES_PATH


MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
EXPERIMENT_NAME = "flight-delay-lakehouse"


FEATURE_COLUMNS = [
    "month",
    "day",
    "day_of_week",
    "hour",
    "season",
    "airline_code",
    "origin",
    "dest",
    "distance",
]

CATEGORICAL_COLUMNS = [
    "season",
    "airline_code",
    "origin",
    "dest",
]

NUMERIC_COLUMNS = [
    "month",
    "day",
    "day_of_week",
    "hour",
    "distance",
]


def load_data():
    print("Loading gold feature table...")

    gold_version = DeltaTable(str(GOLD_FEATURES_PATH)).version()

    df = (
        pl.scan_delta(str(GOLD_FEATURES_PATH))
        .select(FEATURE_COLUMNS + ["arr_delay", "is_delayed"])
        .drop_nulls()
        .collect()
        .to_pandas()
    )

    print(f"Rows loaded: {len(df)}")
    print(f"Gold table version: {gold_version}")

    return df, gold_version


def build_preprocessor():
    return ColumnTransformer(
        transformers=[
            ("cat", OneHotEncoder(handle_unknown="ignore"), CATEGORICAL_COLUMNS),
            ("num", "passthrough", NUMERIC_COLUMNS),
        ]
    )


def train_regression_models(df, gold_version):
    print("Training regression models...")

    X = df[FEATURE_COLUMNS]
    y = df["arr_delay"]

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=42,
    )

    models = {
        "linear_regression": LinearRegression(),
        "random_forest_regressor": RandomForestRegressor(
            n_estimators=50,
            max_depth=12,
            random_state=42,
            n_jobs=-1,
        ),
    }

    for model_name, model in models.items():
        with mlflow.start_run(run_name=model_name):
            pipeline = Pipeline(
                steps=[
                    ("preprocessor", build_preprocessor()),
                    ("model", model),
                ]
            )

            pipeline.fit(X_train, y_train)
            preds = pipeline.predict(X_test)

            mae = mean_absolute_error(y_test, preds)
            mse = mean_squared_error(y_test, preds)
            r2 = r2_score(y_test, preds)

            mlflow.log_param("task", "regression")
            mlflow.log_param("model_name", model_name)
            mlflow.log_param("target", "arr_delay")
            mlflow.log_param("gold_table_version", gold_version)
            mlflow.log_param("test_size", 0.2)

            mlflow.log_metric("mae", mae)
            mlflow.log_metric("mse", mse)
            mlflow.log_metric("r2", r2)

            mlflow.sklearn.log_model(pipeline, artifact_path="model")

            print(f"{model_name}: MAE={mae:.3f}, MSE={mse:.3f}, R2={r2:.3f}")


def train_classification_models(df, gold_version):
    print("Training classification models...")

    X = df[FEATURE_COLUMNS]
    y = df["is_delayed"]

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=42,
        stratify=y,
    )

    models = {
        "logistic_regression": LogisticRegression(
            max_iter=1000,
            n_jobs=-1,
        ),
        "random_forest_classifier": RandomForestClassifier(
            n_estimators=50,
            max_depth=12,
            random_state=42,
            n_jobs=-1,
        ),
    }

    for model_name, model in models.items():
        with mlflow.start_run(run_name=model_name):
            pipeline = Pipeline(
                steps=[
                    ("preprocessor", build_preprocessor()),
                    ("model", model),
                ]
            )

            pipeline.fit(X_train, y_train)
            preds = pipeline.predict(X_test)

            accuracy = accuracy_score(y_test, preds)
            f1 = f1_score(y_test, preds)

            mlflow.log_param("task", "classification")
            mlflow.log_param("model_name", model_name)
            mlflow.log_param("target", "is_delayed")
            mlflow.log_param("gold_table_version", gold_version)
            mlflow.log_param("test_size", 0.2)

            mlflow.log_metric("accuracy", accuracy)
            mlflow.log_metric("f1", f1)

            mlflow.sklearn.log_model(pipeline, artifact_path="model")

            print(f"{model_name}: accuracy={accuracy:.3f}, f1={f1:.3f}")


def main():
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    df, gold_version = load_data()

    train_regression_models(df, gold_version)
    train_classification_models(df, gold_version)

    print("ML training completed.")


if __name__ == "__main__":
    main()