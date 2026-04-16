import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

def generate_platform_transactions():
    """
    Generate platform transactions with 4 injected anomalies:
    1. Transaction that settles next month (T0099 on Jan 31)
    2. Rounding difference: three transactions summing to $1.00 but bank rounds each to $0.33
    3. Duplicate transaction (T0001 appears twice)
    4. Refund with no matching original (R9999 has no original_txn_id)
    """
    txn_ids = [f"T{i:04d}" for i in range(1, 101)]
    amounts = [round(np.random.uniform(5, 500), 2) for _ in range(100)]
    timestamps = [datetime(2024, 1, 1) + timedelta(days=random.randint(0, 29)) for _ in range(100)]
    types = ['payment'] * 100
    original_txn_ids = [None] * 100  # for refund linkage

    # 1. Transaction that settles next month (T0099)
    txn_ids[98] = "T0099"
    timestamps[98] = datetime(2024, 1, 31)

    # 2. Rounding diff: three transactions (T0100, T0101, T0102) platform totals $1.00
    # Replace last normal transaction (index 99) with 0.34
    amounts[99] = 0.34  # T0100
    txn_ids.append("T0101")
    amounts.append(0.33)
    timestamps.append(datetime(2024, 1, 15))
    types.append('payment')
    original_txn_ids.append(None)
    txn_ids.append("T0102")
    amounts.append(0.33)
    timestamps.append(datetime(2024, 1, 15))
    types.append('payment')
    original_txn_ids.append(None)

    # 3. Duplicate: add a duplicate of T0001
    txn_ids.append("T0001")
    amounts.append(150.00)
    timestamps.append(datetime(2024, 1, 5))
    types.append('payment')
    original_txn_ids.append(None)

    # 4. Orphan refund: refund without original_txn_id
    txn_ids.append("R9999")
    amounts.append(-75.00)
    timestamps.append(datetime(2024, 1, 25))
    types.append('refund')
    original_txn_ids.append(None)  # missing link

    df = pd.DataFrame({
        'txn_id': txn_ids,
        'amount': amounts,
        'timestamp': timestamps,
        'type': types,
        'original_txn_id': original_txn_ids
    })
    return df

def generate_bank_settlements(platform_df):
    settlements = []
    for _, row in platform_df.iterrows():
        txn_id = row['txn_id']
        amount = row['amount']
        txn_date = row['timestamp']
        # Normal settlement: 1-2 days later
        settle_date = txn_date + timedelta(days=random.choice([1, 2]))
        # Anomaly 1: T0099 settles in February
        if txn_id == "T0099":
            settle_date = datetime(2024, 2, 1)
        # Anomaly 2: rounding – bank records T0100, T0101, T0102 as 0.33 each
        if txn_id in ["T0100", "T0101", "T0102"]:
            amount = 0.33
        settlements.append({
            'txn_id': txn_id,
            'amount': amount,
            'settlement_date': settle_date
        })
    # Add duplicate in bank dataset: duplicate T0002
    settlements.append({
        'txn_id': 'T0002',
        'amount': 125.00,
        'settlement_date': datetime(2024, 1, 6)
    })
    return pd.DataFrame(settlements)

if __name__ == '__main__':
    plat = generate_platform_transactions()
    bank = generate_bank_settlements(plat)
    plat.to_csv('platform_transactions.csv', index=False)
    bank.to_csv('bank_settlements.csv', index=False)
    print("Generated CSV files with anomalies.")
    print(f"Platform rows: {len(plat)}")
    print(f"Bank rows: {len(bank)}")