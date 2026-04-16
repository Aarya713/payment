import pytest
import pandas as pd
from datetime import datetime
from reconcile import (
    detect_next_month_settlement,
    detect_rounding_differences,
    detect_duplicates_platform,
    detect_duplicates_bank,
    detect_orphan_refunds
)

def test_next_month_settlement():
    plat = pd.DataFrame({
        'txn_id': ['T1', 'T2'],
        'amount': [100.0, 200.0],
        'timestamp': pd.to_datetime(['2024-01-31', '2024-01-15']),
        'type': ['payment', 'payment'],
        'original_txn_id': [None, None]
    })
    bank = pd.DataFrame({
        'txn_id': ['T1', 'T2'],
        'amount': [100.0, 200.0],
        'settlement_date': pd.to_datetime(['2024-02-01', '2024-01-16'])
    })
    result = detect_next_month_settlement(plat, bank)
    assert len(result) == 1
    assert result.iloc[0]['txn_id'] == 'T1'

def test_rounding_differences():
    plat = pd.DataFrame({
        'txn_id': ['A', 'B', 'C'],
        'amount': [0.34, 0.33, 0.33],
        'timestamp': pd.to_datetime(['2024-01-01'] * 3),
        'type': ['payment'] * 3,
        'original_txn_id': [None] * 3
    })
    bank = pd.DataFrame({
        'txn_id': ['A', 'B', 'C'],
        'amount': [0.33, 0.33, 0.33],
        'settlement_date': pd.to_datetime(['2024-01-02'] * 3)
    })
    total_diff, mismatches = detect_rounding_differences(plat, bank)
    # 0.34+0.33+0.33 = 1.00; bank sum = 0.99; diff = 0.01
    assert total_diff == 0.01
    assert len(mismatches) == 1  # only transaction A mismatches

def test_duplicates_platform():
    plat = pd.DataFrame({
        'txn_id': ['D1', 'D1', 'D2'],
        'amount': [10.0, 10.0, 20.0],
        'timestamp': pd.to_datetime(['2024-01-01', '2024-01-01', '2024-01-02']),
        'type': ['payment'] * 3,
        'original_txn_id': [None] * 3
    })
    dups = detect_duplicates_platform(plat)
    assert len(dups) == 2  # both D1 rows
    assert dups['txn_id'].iloc[0] == 'D1'

def test_duplicates_bank():
    bank = pd.DataFrame({
        'txn_id': ['D1', 'D1', 'D2'],
        'amount': [10.0, 10.0, 20.0],
        'settlement_date': pd.to_datetime(['2024-01-02', '2024-01-02', '2024-01-03'])
    })
    dups = detect_duplicates_bank(bank)
    assert len(dups) == 2

def test_orphan_refunds():
    plat = pd.DataFrame({
        'txn_id': ['R1', 'R2', 'P1'],
        'amount': [-20.0, -10.0, 20.0],
        'timestamp': pd.to_datetime(['2024-01-10', '2024-01-11', '2024-01-09']),
        'type': ['refund', 'refund', 'payment'],
        'original_txn_id': ['P1', None, None]  # R1 has link, R2 orphan
    })
    orphans = detect_orphan_refunds(plat)
    assert len(orphans) == 1
    assert orphans.iloc[0]['txn_id'] == 'R2'