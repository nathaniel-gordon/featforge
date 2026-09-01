"""Tests for featforge feature engineering and data preparation pipelines."""

import pandas as pd
import numpy as np
import pytest

from src.feature_engineering.rfm_features import build_rfm_features
from src.feature_engineering.behavior_features import build_behavior_features
from src.data_prep.cutoffs import generate_cutoff_dates
from src.data_prep.labels import build_purchase_labels


@pytest.fixture
def sample_transactions():
    """Generates synthetic transactional DataFrame with all cleaned columns."""
    dates = pd.date_range(start="2023-01-01", end="2023-06-30", freq="D")
    records = []
    for i, d in enumerate(dates):
        qty = (i % 5) + 1
        price = 10.0 + (i % 3) * 5.0
        records.append({
            "InvoiceNo": f"INV{i:04d}",
            "CustomerID": int(i % 10),
            "InvoiceDate": d,
            "Quantity": qty,
            "UnitPrice": price,
            "TotalPrice": qty * price,
            "Revenue": qty * price,
            "StockCode": f"SKU_{i % 5}",
            "Description": f"Product_{i % 8}",
            "Country": "United Kingdom",
            "is_cancellation": False,
        })
    return pd.DataFrame(records)


def test_build_rfm_features(sample_transactions):
    cutoff = pd.Timestamp("2023-04-01")
    rfm = build_rfm_features(sample_transactions, cutoff=cutoff, feature_window=60)
    assert not rfm.empty
    assert "CustomerID" in rfm.columns
    assert "recency_days" in rfm.columns
    assert "frequency" in rfm.columns
    assert "monetary" in rfm.columns
    assert (rfm["frequency"] > 0).all()


def test_build_behavior_features(sample_transactions):
    cutoff = pd.Timestamp("2023-04-01")
    behavior = build_behavior_features(sample_transactions, cutoff=cutoff, feature_window=60)
    assert not behavior.empty
    assert "CustomerID" in behavior.columns
    assert len(behavior.columns) >= 3


def test_generate_cutoff_dates(sample_transactions):
    cutoffs = generate_cutoff_dates(
        sample_transactions,
        feature_window=30,
        purchase_window=30,
        step=15
    )
    assert len(cutoffs) > 0
    assert all(isinstance(c, pd.Timestamp) for c in cutoffs)


def test_build_purchase_labels(sample_transactions):
    cutoff = pd.Timestamp("2023-04-01")
    labels = build_purchase_labels(
        sample_transactions,
        cutoff=cutoff,
        purchase_window=30,
        feature_window=60
    )
    assert not labels.empty
    assert "CustomerID" in labels.columns
    assert "purchased" in labels.columns
