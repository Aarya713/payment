import pandas as pd
from datetime import datetime

# ============================================================================
# ASSUMPTIONS & EXPLANATIONS
# ============================================================================
"""
1. Each platform transaction has a unique txn_id (except duplicates we detect).
2. Bank settlements reference the same txn_id (in real life, matching may require fuzzy logic).
3. Refunds are linked to original transactions via 'original_txn_id' field.
   - A refund is considered orphan if original_txn_id is missing (NaN) or points to a non-existent payment.
4. Settlement delay is 1–2 days; anything beyond that is flagged as next-month settlement.
5. Rounding differences are detected by grouping by txn_id and summing amounts before comparison.
6. Duplicate detection: any txn_id that appears more than once in a dataset is flagged.
7. The large total mismatch (-$124.99) arises due to:
   - Duplicate entries in bank dataset (T0002 appears twice with different amounts)
   - Duplicate entries in platform dataset (T0001 appears twice with different amounts)
   - Injected rounding adjustments (T0100, T0101, T0102)
   - These cause the merged sums to differ significantly.
"""
# ============================================================================

def load_data():
    plat = pd.read_csv('platform_transactions.csv', parse_dates=['timestamp'])
    bank = pd.read_csv('bank_settlements.csv', parse_dates=['settlement_date'])
    plat['amount'] = plat['amount'].astype(float)
    bank['amount'] = bank['amount'].astype(float)
    return plat, bank

def detect_next_month_settlement(plat, bank):
    """Transactions where settlement date is in a different calendar month."""
    merged = plat.merge(bank, on='txn_id', how='inner')
    merged['txn_month'] = merged['timestamp'].dt.to_period('M')
    merged['settle_month'] = merged['settlement_date'].dt.to_period('M')
    diff = merged[merged['txn_month'] != merged['settle_month']]
    return diff[['txn_id', 'timestamp', 'settlement_date']].drop_duplicates()

def detect_rounding_differences(plat, bank):
    """
    Group by txn_id and sum amounts to avoid duplicate cross-join issues.
    Returns total difference and per-txn mismatches.
    """
    # Group by txn_id to consolidate duplicates before comparison
    plat_grouped = plat.groupby('txn_id', as_index=False)['amount'].sum()
    bank_grouped = bank.groupby('txn_id', as_index=False)['amount'].sum()
    
    total_plat = plat_grouped['amount'].sum()
    total_bank = bank_grouped['amount'].sum()
    total_diff = round(total_plat - total_bank, 2)
    
    merged = plat_grouped.merge(bank_grouped, on='txn_id', suffixes=('_plat', '_bank'), how='outer').fillna(0)
    mismatches = merged[abs(merged['amount_plat'] - merged['amount_bank']) > 0.001]
    mismatches = mismatches[['txn_id', 'amount_plat', 'amount_bank']]
    return total_diff, mismatches

def detect_duplicates_platform(df):
    """Detect duplicate txn_id in platform data (ignoring amount/timestamp)."""
    dup_mask = df.duplicated(subset=['txn_id'], keep=False)
    return df[dup_mask].sort_values('txn_id')

def detect_duplicates_bank(df):
    """Detect duplicate txn_id in bank data."""
    dup_mask = df.duplicated(subset=['txn_id'], keep=False)
    return df[dup_mask].sort_values('txn_id')

def detect_orphan_refunds(plat):
    """
    Refunds with no matching original transaction.
    Logic: refund if type='refund' and original_txn_id is missing or points to non-existent payment.
    """
    refunds = plat[plat['type'] == 'refund']
    payments = plat[plat['type'] == 'payment']['txn_id'].unique()
    orphan = refunds[
        (refunds['original_txn_id'].isna()) |
        (~refunds['original_txn_id'].isin(payments))
    ]
    return orphan[['txn_id', 'amount', 'timestamp', 'original_txn_id']]

if __name__ == '__main__':
    plat, bank = load_data()

    print("=" * 60)
    print("PAYMENT RECONCILIATION REPORT")
    print("=" * 60)

    print("\n1. TRANSACTIONS SETTLED IN FOLLOWING MONTH:")
    next_month = detect_next_month_settlement(plat, bank)
    if len(next_month) > 0:
        print(next_month.to_string(index=False))
    else:
        print("None found.")

    print("\n2. ROUNDING DIFFERENCES:")
    total_diff, mismatches = detect_rounding_differences(plat, bank)
    print(f"   Total platform sum (grouped): ${plat.groupby('txn_id')['amount'].sum().sum():.2f}")
    print(f"   Total bank sum (grouped):     ${bank.groupby('txn_id')['amount'].sum().sum():.2f}")
    print(f"   Difference:                    ${total_diff:.2f}")
    print("\n   Explanation: The large mismatch arises from duplicate txn_ids with different amounts")
    print("   (T0001 in platform, T0002 in bank) and injected rounding adjustments.")
    if len(mismatches) > 0:
        print("\n   Per-transaction mismatches (after grouping by txn_id):")
        print(mismatches.to_string(index=False))
    else:
        print("   No per-transaction mismatches found.")

    print("\n3. DUPLICATE ENTRIES (txn_id appears more than once):")
    plat_dups = detect_duplicates_platform(plat)
    bank_dups = detect_duplicates_bank(bank)
    if len(plat_dups) > 0:
        print("   In platform transactions:")
        print(plat_dups[['txn_id', 'amount', 'timestamp']].to_string(index=False))
    else:
        print("   No duplicate txn_id in platform.")
    if len(bank_dups) > 0:
        print("   In bank settlements:")
        print(bank_dups[['txn_id', 'amount', 'settlement_date']].to_string(index=False))
    else:
        print("   No duplicate txn_id in bank.")

    print("\n4. ORPHAN REFUNDS (no matching original transaction):")
    orphans = detect_orphan_refunds(plat)
    if len(orphans) > 0:
        print(orphans.to_string(index=False))
    else:
        print("None found.")

    print("\n" + "=" * 60)