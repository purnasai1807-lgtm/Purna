from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd
from pandas.api.types import is_datetime64_any_dtype, is_numeric_dtype
from sklearn.cluster import KMeans
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    mean_absolute_error,
    mean_squared_error,
    precision_score,
    r2_score,
    recall_score,
    silhouette_score,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


def build_one_hot_encoder() -> OneHotEncoder:
    try:
        return OneHotEncoder(handle_unknown="ignore", sparse_output=False)
    except TypeError:
        return OneHotEncoder(handle_unknown="ignore", sparse=False)


def build_modeling_summary(dataframe: pd.DataFrame, target_column: str | None) -> dict[str, Any]:
    prepared_frame = prepare_modeling_frame(dataframe)
    if prepared_frame.shape[0] < 8:
        return {
            "status": "skipped",
            "mode": "insufficient_data",
            "suggestions": [],
            "reason": "At least 8 records are recommended before training a baseline machine learning model.",
        }

    if target_column:
        if target_column not in prepared_frame.columns:
            return {
                "status": "skipped",
                "mode": "target_missing",
                "suggestions": [],
                "reason": f"Target column '{target_column}' is not available after cleaning.",
            }

        target_series = prepared_frame[target_column]
        features = prepared_frame.drop(columns=[target_column])
        if features.empty:
            return {
                "status": "skipped",
                "mode": "missing_features",
                "suggestions": [],
                "reason": "The dataset needs at least one feature column in addition to the target.",
            }

        if infer_target_mode(target_series) == "regression":
            return run_regression_workflow(features, target_series, target_column)
        return run_classification_workflow(features, target_series, target_column)

    return run_clustering_workflow(prepared_frame)


def prepare_modeling_frame(dataframe: pd.DataFrame) -> pd.DataFrame:
    frame = dataframe.copy()
    for column in frame.columns:
        if is_datetime64_any_dtype(frame[column]):
            frame[column] = frame[column].astype("int64") // 10**9
    return frame


def infer_target_mode(target_series: pd.Series) -> str:
    if is_numeric_dtype(target_series) and target_series.nunique(dropna=True) > max(10, int(len(target_series) * 0.1)):
        return "regression"
    return "classification"


def build_preprocessor(feature_frame: pd.DataFrame) -> tuple[ColumnTransformer, list[str], list[str]]:
    numeric_features = list(feature_frame.select_dtypes(include=[np.number]).columns)
    categorical_features = [column for column in feature_frame.columns if column not in numeric_features]

    transformers = []
    if numeric_features:
        transformers.append(
            (
                "num",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="median")),
                        ("scaler", StandardScaler()),
                    ]
                ),
                numeric_features,
            )
        )
    if categorical_features:
        transformers.append(
            (
                "cat",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        ("encoder", build_one_hot_encoder()),
                    ]
                ),
                categorical_features,
            )
        )

    if not transformers:
        raise ValueError("No valid features are available for modeling.")

    return ColumnTransformer(transformers=transformers), numeric_features, categorical_features


def run_regression_workflow(
    features: pd.DataFrame,
    target: pd.Series,
    target_column: str,
) -> dict[str, Any]:
    preprocessor, _, _ = build_preprocessor(features)
    model = RandomForestRegressor(n_estimators=250, random_state=42)
    pipeline = Pipeline([("preprocess", preprocessor), ("model", model)])

    x_train, x_test, y_train, y_test = train_test_split(
        features,
        target,
        test_size=0.2,
        random_state=42,
    )
    pipeline.fit(x_train, y_train)
    predictions = pipeline.predict(x_test)

    metrics = {
        "rmse": round(float(math.sqrt(mean_squared_error(y_test, predictions))), 4),
        "mae": round(float(mean_absolute_error(y_test, predictions)), 4),
        "r2": round(float(r2_score(y_test, predictions)), 4),
    }

    return {
        "status": "completed",
        "mode": "regression",
        "target_column": target_column,
        "selected_model": "Random Forest Regressor",
        "suggestions": [
            {
                "name": "Random Forest Regressor",
                "rationale": "Strong baseline for non-linear numeric prediction with mixed features.",
            },
            {
                "name": "Gradient Boosting Regressor",
                "rationale": "Useful when you need sharper performance on tabular business data.",
            },
            {
                "name": "Linear Regression",
                "rationale": "Best for simple interpretability and low-latency scoring.",
            },
        ],
        "metrics": metrics,
        "feature_importance": extract_feature_importance(pipeline),
        "notes": "A numeric target was detected, so the app trained a baseline regression model.",
    }


def run_classification_workflow(
    features: pd.DataFrame,
    target: pd.Series,
    target_column: str,
) -> dict[str, Any]:
    if target.nunique(dropna=True) < 2:
        return {
            "status": "skipped",
            "mode": "classification",
            "suggestions": [],
            "reason": "Classification needs at least two distinct target classes.",
        }

    preprocessor, _, _ = build_preprocessor(features)
    model = RandomForestClassifier(n_estimators=250, random_state=42, class_weight="balanced")
    pipeline = Pipeline([("preprocess", preprocessor), ("model", model)])

    target_as_text = target.astype(str)
    test_size = 0.2
    estimated_test_rows = max(1, int(round(len(target_as_text) * test_size)))
    can_stratify = (
        target_as_text.value_counts().min() >= 2
        and target_as_text.nunique() <= estimated_test_rows
    )
    stratify_target = target_as_text if can_stratify else None
    x_train, x_test, y_train, y_test = train_test_split(
        features,
        target_as_text,
        test_size=test_size,
        random_state=42,
        stratify=stratify_target,
    )
    pipeline.fit(x_train, y_train)
    predictions = pipeline.predict(x_test)

    metrics = {
        "accuracy": round(float(accuracy_score(y_test, predictions)), 4),
        "precision": round(float(precision_score(y_test, predictions, average="weighted", zero_division=0)), 4),
        "recall": round(float(recall_score(y_test, predictions, average="weighted", zero_division=0)), 4),
        "f1_score": round(float(f1_score(y_test, predictions, average="weighted", zero_division=0)), 4),
    }

    return {
        "status": "completed",
        "mode": "classification",
        "target_column": target_column,
        "selected_model": "Random Forest Classifier",
        "suggestions": [
            {
                "name": "Random Forest Classifier",
                "rationale": "Reliable baseline for mixed-type categorical predictions.",
            },
            {
                "name": "Logistic Regression",
                "rationale": "Fast and interpretable for cleaner, linearly separable classes.",
            },
            {
                "name": "Gradient Boosting Classifier",
                "rationale": "Great for squeezing more accuracy out of rich tabular datasets.",
            },
        ],
        "metrics": metrics,
        "class_distribution": sanitize_simple(target_as_text.value_counts().to_dict()),
        "feature_importance": extract_feature_importance(pipeline),
        "notes": "A categorical target was detected, so the app trained a baseline classification model.",
    }


def run_clustering_workflow(dataframe: pd.DataFrame) -> dict[str, Any]:
    try:
        preprocessor, _, _ = build_preprocessor(dataframe)
    except ValueError as exc:
        return {"status": "skipped", "mode": "clustering", "suggestions": [], "reason": str(exc)}

    transformed = preprocessor.fit_transform(dataframe)
    if len(dataframe) < 5:
        return {
            "status": "skipped",
            "mode": "clustering",
            "suggestions": [],
            "reason": "Clustering works best once you have at least five records.",
        }

    best_score = -1.0
    best_cluster_count = None
    best_labels = None

    max_clusters = min(8, len(dataframe) - 1)
    for cluster_count in range(2, max_clusters + 1):
        candidate_model = KMeans(n_clusters=cluster_count, random_state=42, n_init=10)
        labels = candidate_model.fit_predict(transformed)
        if len(set(labels)) < 2:
            continue
        score = float(silhouette_score(transformed, labels))
        if score > best_score:
            best_score = score
            best_cluster_count = cluster_count
            best_labels = labels

    if best_cluster_count is None or best_labels is None:
        return {
            "status": "skipped",
            "mode": "clustering",
            "suggestions": [],
            "reason": "The dataset does not separate into meaningful clusters yet.",
        }

    cluster_sizes = pd.Series(best_labels).value_counts().sort_index().to_dict()
    return {
        "status": "completed",
        "mode": "clustering",
        "selected_model": "KMeans",
        "suggestions": [
            {
                "name": "KMeans",
                "rationale": "Fast baseline when you want grouped segments without a target label.",
            },
            {
                "name": "Agglomerative Clustering",
                "rationale": "Useful when nested or hierarchical groups may exist.",
            },
            {
                "name": "DBSCAN",
                "rationale": "Helpful when clusters may be irregular or include noise points.",
            },
        ],
        "metrics": {"silhouette_score": round(best_score, 4)},
        "cluster_summary": sanitize_simple(cluster_sizes),
        "notes": "No target column was selected, so the app explored unsupervised segmentation.",
    }


def extract_feature_importance(pipeline: Pipeline, top_n: int = 8) -> list[dict[str, Any]]:
    model = pipeline.named_steps["model"]
    if not hasattr(model, "feature_importances_"):
        return []

    preprocessor = pipeline.named_steps["preprocess"]
    try:
        feature_names = list(preprocessor.get_feature_names_out())
    except Exception:
        feature_names = [f"feature_{index + 1}" for index in range(len(model.feature_importances_))]

    scored_features = sorted(
        zip(feature_names, model.feature_importances_),
        key=lambda item: item[1],
        reverse=True,
    )[:top_n]

    return [
        {
            "feature": feature_name.replace("num__", "").replace("cat__", ""),
            "importance": round(float(importance), 4),
        }
        for feature_name, importance in scored_features
    ]


def sanitize_simple(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): sanitize_simple(item) for key, item in value.items()}
    if isinstance(value, list):
        return [sanitize_simple(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        return round(float(value), 4)
    return value
