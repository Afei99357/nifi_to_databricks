"""CLI to compute a t-SNE embedding over flattened processor features."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

import matplotlib.pyplot as plt
import pandas as pd
from sklearn.manifold import TSNE
from sklearn.preprocessing import StandardScaler

DEFAULT_INPUT = "derived_classification_features_all.csv"
DEFAULT_OUTPUT = "derived_classification_tsne.png"


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        default=DEFAULT_INPUT,
        help="CSV file with flattened processor features",
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help="Path to save the t-SNE scatter plot (PNG)",
    )
    parser.add_argument(
        "--color-by",
        choices=["migration_category", "short_type"],
        default="migration_category",
        help="Column used to color the scatter plot",
    )
    parser.add_argument(
        "--perplexity",
        type=float,
        default=30.0,
        help="t-SNE perplexity parameter",
    )
    parser.add_argument(
        "--learning-rate",
        type=float,
        default=200.0,
        help="t-SNE learning rate",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility",
    )
    return parser.parse_args(argv)


EXCLUDE_COLUMNS = {
    "template",
    "processor_id",
    "processor_name",
    "processor_type",
    "short_type",
    "parent_group",
    "migration_category",
    "databricks_target",
    "classification_source",
    "rule",
    "promotion_applied",
    "confidence",
}


def load_feature_matrix(
    path: Path, *, label_column: str
) -> tuple[pd.DataFrame, pd.Series]:
    df = pd.read_csv(path)
    if label_column not in df.columns:
        raise ValueError(f"Input CSV must contain a '{label_column}' column")

    labels = df[label_column].fillna("(none)").astype(str)
    feature_columns = [
        col
        for col in df.columns
        if col not in EXCLUDE_COLUMNS and pd.api.types.is_numeric_dtype(df[col])
    ]
    features = df[feature_columns].fillna(0.0)
    return features, labels


def compute_tsne(
    features: pd.DataFrame,
    *,
    perplexity: float,
    learning_rate: float,
    seed: int,
) -> pd.DataFrame:
    scaler = StandardScaler()
    scaled = scaler.fit_transform(features)
    tsne = TSNE(
        n_components=2,
        perplexity=perplexity,
        learning_rate=learning_rate,
        random_state=seed,
        init="pca",
    )
    embedding = tsne.fit_transform(scaled)
    return pd.DataFrame(embedding, columns=["tsne_x", "tsne_y"], index=features.index)


def plot_embedding(
    embedding: pd.DataFrame,
    labels: pd.Series,
    color_by: str,
    output_path: Path,
) -> None:
    fig, ax = plt.subplots(figsize=(10, 8))
    categories = sorted(labels.unique())
    for category in categories:
        mask = labels == category
        ax.scatter(
            embedding.loc[mask, "tsne_x"],
            embedding.loc[mask, "tsne_y"],
            label=category,
            s=12,
            alpha=0.7,
        )
    ax.set_title(f"t-SNE embedding colored by {color_by}")
    ax.set_xlabel("t-SNE 1")
    ax.set_ylabel("t-SNE 2")
    if color_by == "short_type" and len(categories) > 10:
        legend = ax.legend(
            loc="upper right",
            bbox_to_anchor=(1.25, 1.0),
            fontsize="x-small",
            ncol=2,
            frameon=False,
        )
    else:
        legend = ax.legend(loc="best")
    ax.grid(True, alpha=0.2)
    fig.tight_layout()
    fig.savefig(output_path, dpi=180)
    plt.close(fig)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    input_path = Path(args.input)
    if not input_path.exists():
        raise SystemExit(f"Input CSV not found: {input_path}")

    features, labels = load_feature_matrix(input_path, label_column=args.color_by)
    embedding = compute_tsne(
        features,
        perplexity=args.perplexity,
        learning_rate=args.learning_rate,
        seed=args.seed,
    )
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plot_embedding(embedding, labels, args.color_by, output_path)
    print(
        f"t-SNE computed for {len(features)} processors. Plot saved to {output_path}."
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
