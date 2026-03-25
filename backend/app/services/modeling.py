from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd
from pandas.api.types import (
    is_datetime64_any_dtype,
    is_float_dtype,
    is_numeric_dtype,
)
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

MAX_MODELING_ROWS = 20000
MAX_CLUSTERING_ROWS = 10000
MAX_CLASSIFICATION_CLASSES = 40
MAX_CLASSIFICATION_CLASS_RATIO = 0.08
RANDOM_STATE = 42


def build_one_hot_encoder() -> OneHotEncoder:
    try:
        return OneHotEncoder(handle_unknown="ignore", sparse_output=False)
    except TypeError:
        return OneHotEncoder(handle_unknown="ignore", sparse=False)


def build_modeling_summary(
    dataframe: pd.DataFrame,
    target_column: str | None,
) -> dict[str, Any]:
    prepared_frame = prepare_modeling_frame(dataframe)
    if prepared_frame.shape[0] < 8:
        return {
            "status": "skipped",
            "mode": "insufficient_data",
            "suggestions": [],
            "reason": (
                "At least 8 records are recommended before training a baseline "
                "machine learning model."
            ),
        }

    if target_column:
        if target_column not in prepared_frame.columns:
            return {
                "status": "skipped",
                "mode": "target_missing",
                "suggestions": [],
                "reason": (
                    f"Target column '{target_column}' is not available after cleaning."
                ),
            }

        target_series = prepared_frame[target_column]
        features = prepared_frame.drop(columns=[target_column])
        if features.empty:
            return {
                "status": "skipped",
                "mode": "missing_features",
                "suggestions": [],
                "reason": (
                    "The dataset needs at least one feature column in addition to "
                    "the target."
                ),
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
    non_null_target = target_series.dropna()
    unique_values = non_null_target.nunique(dropna=True)

    if is_numeric_dtype(non_null_target):
        unique_ratio = unique_values / max(len(non_null_target), 1)
        if is_float_dtype(non_null_target):
            return "regression"
        if unique_values >= 20 and unique_ratio >= 0.005:
            return "regression"

    return "classification"


def sample_rows(
    features: pd.DataFrame,
    target: pd.Series | None = None,
    max_rows: int = MAX_MODELING_ROWS,
) -> tuple[pd.DataFrame, pd.Series | None]:
    if len(features) <= max_rows:
        return features, target

    sampled_features = features.sample(n=max_rows, random_state=RANDOM_STATE)
    if target is None:
        return sampled_features, None

    sampled_target = target.loc[sampled_features.index]
    return sampled_features, sampled_target


def build_preprocessor(
    feature_frame: pd.DataFrame,
) -> tuple[ColumnTransformer, list[str], list[str]]:
    numeric_features = list(feature_frame.select_dtypes(include=[np.number]).columns)
    categorical_features = [
        column for column in feature_frame.columns if column not in numeric_features
    ]
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
    sampled_features, sampled_target = sample_rows(features, target)
    preprocessor, _, _ = build_preprocessor(features)
    model = RandomForestRegressor(
        n_estimators=120,
        max_depth=18,
        random_state=RANDOM_STATE,
        n_jobs=1,
    )
    pipeline = Pipeline([("preprocess", preprocessor), ("model", model)])
    x_train, x_test, y_train, y_test = train_test_split(
        sampled_features,
        sampled_target,
        test_size=0.2,
        random_state=RANDOM_STATE,
    )

    try:
        pipeline.fit(x_train, y_train)
        predictions = pipeline.predict(x_test)
    except MemoryError:
        return {
            "status": "skipped",
            "mode": "regression",
            "target_column": target_column,
            "suggestions": [],
            "reason": (
                "This dataset is too large for in-request regression modeling. "
                "The analysis report was generated without a trained model."
            ),
        }

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
                "rationale": (
                    "Strong baseline for non-linear numeric prediction with mixed "
                    "features."
                ),
            },
            {
                "name": "Gradient Boosting Regressor",
                "rationale": (
                    "Useful when you need sharper performance on tabular business "
                    "data."
                ),
            },
            {
                "name": "Linear Regression",
                "rationale": "Best for simple interpretability and low-latency scoring.",
            },
        ],
        "metrics": metrics,
        "feature_importance": extract_feature_importance(pipeline),
        "notes": (
            "A numeric target was detected, so the app trained a baseline "
            "regression model."
        ),
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

    target_as_text = target.astype(str)
    unique_classes = int(target_as_text.nunique())
    class_ratio = unique_classes / max(len(target_as_text), 1)
    if (
        unique_classes > MAX_CLASSIFICATION_CLASSES
        or class_ratio > MAX_CLASSIFICATION_CLASS_RATIO
    ):
        return {
            "status": "skipped",
            "mode": "classification",
            "target_column": target_column,
            "suggestions": [],
            "reason": (
                "The selected target has too many distinct classes for a stable "
                "baseline classifier. Choose a simpler categorical target or leave "
                "the target blank."
            ),
        }

    sampled_features, sampled_target = sample_rows(features, target_as_text)
    preprocessor, _, _ = build_preprocessor(sampled_features)
    model = RandomForestClassifier(
        n_estimators=120,
        max_depth=18,
        random_state=RANDOM_STATE,
        class_weight="balanced",
        n_jobs=1,
    )
    pipeline = Pipeline([("preprocess", preprocessor), ("model", model)])
    test_size = 0.2
    estimated_test_rows = max(1, int(round(len(sampled_target) * test_size)))
    can_stratify = (
        sampled_target.value_counts().min() >= 2
        and sampled_target.nunique() <= estimated_test_rows
    )
    stratify_target = sampled_target if can_stratify else None
    x_train, x_test, y_train, y_test = train_test_split(
        sampled_features,
        sampled_target,
        test_size=test_size,
        random_state=RANDOM_STATE,
        stratify=stratify_target,
    )

    try:
        pipeline.fit(x_train, y_train)
        predictions = pipeline.predict(x_test)
    except MemoryError:
        return {
            "status": "skipped",
            "mode": "classification",
            "target_column": target_column,
            "suggestions": [],
            "reason": (
                "This dataset is too large for in-request classification modeling. "
                "The analysis report was generated without a trained classifier."
            ),
        }

    metrics = {
        "accuracy": round(float(accuracy_score(y_test, predictions)), 4),
        "precision": round(
            float(
                precision_score(
                    y_test,
                    predictions,
                    average="weighted",
                    zero_division=0,
                )
            ),
            4,
        ),
        "recall": round(
            float(
                recall_score(
                    y_test,
                    predictions,
                    average="weighted",
                    zero_division=0,
                )
            ),
            4,
        ),
        "f1_score": round(
            float(
                f1_score(
                    y_test,
                    predictions,
                    average="weighted",
                    zero_division=0,
                )
            ),
            4,
        ),
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
                "rationale": (
                    "Fast and interpretable for cleaner, linearly separable classes."
                ),
            },
            {
                "name": "Gradient Boosting Classifier",
                "rationale": (
                    "Great for squeezing more accuracy out of rich tabular datasets."
                ),
            },
        ],
        "metrics": metrics,
        "class_distribution": sanitize_simple(target_as_text.value_counts().to_dict()),
        "feature_importance": extract_feature_importance(pipeline),
        "notes": (
            "A categorical target was detected, so the app trained a baseline "
            "classification model."
        ),
    }


def run_clustering_workflow(dataframe: pd.DataFrame) -> dict[str, Any]:
    clustering_frame = (
        dataframe.sample(n=MAX_CLUSTERING_ROWS, random_state=RANDOM_STATE)
        if len(dataframe) > MAX_CLUSTERING_ROWS
        else dataframe
    )
    try:
        preprocessor, _, _ = build_preprocessor(clustering_frame)
    except ValueError as exc:
        return {
            "status": "skipped",
            "mode": "clustering",
            "suggestions": [],
            "reason": str(exc),
        }

    transformed = preprocessor.fit_transform(clustering_frame)
    if len(clustering_frame) < 5:
        return {
            "status": "skipped",
            "mode": "clustering",
            "suggestions": [],
            "reason": "Clustering works best once you have at least five records.",
        }

    best_score = -1.0
    best_cluster_count = None
    best_labels = None
    max_clusters = min(8, len(clustering_frame) - 1)
    for cluster_count in range(2, max_clusters + 1):
        candidate_model = KMeans(
            n_clusters=cluster_count,
            random_state=RANDOM_STATE,
            n_init=10,
        )
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
                "rationale": (
                    "Fast baseline when you want grouped segments without a target "
                    "label."
                ),
            },
            {
                "name": "Agglomerative Clustering",
                "rationale": "Useful when nested or hierarchical groups may exist.",
            },
            {
                "name": "DBSCAN",
                "rationale": (
                    "Helpful when clusters may be irregular or include noise points."
                ),
            },
        ],
        "metrics": {"silhouette_score": round(best_score, 4)},
        "cluster_summary": sanitize_simple(cluster_sizes),
        "notes": (
            "No target column was selected, so the app explored unsupervised "
            "segmentation."
        ),
    }


def extract_feature_importance(
    pipeline: Pipeline,
    top_n: int = 8,
) -> list[dict[str, Any]]:
    model = pipeline.named_steps["model"]
    if not hasattr(model, "feature_importances_"):
        return []

    preprocessor = pipeline.named_steps["preprocess"]
    try:
        feature_names = list(preprocessor.get_feature_names_out())
    except Exception:
        feature_names = [
            f"feature_{index + 1}"
            for index in range(len(model.feature_importances_))
        ]

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
