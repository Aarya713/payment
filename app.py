import streamlit as st
import pandas as pd
import plotly.express as px
from reconcile import (
    detect_next_month_settlement,
    detect_rounding_differences,
    detect_duplicates_platform,
    detect_duplicates_bank,
    detect_orphan_refunds
)

st.set_page_config(page_title="Payment Reconciliation Dashboard", layout="wide")
st.title("💰 Payment Reconciliation Dashboard")
st.markdown("Upload your platform transactions and bank settlements CSV files to detect anomalies.")

col1, col2 = st.columns(2)
with col1:
    plat_file = st.file_uploader("Upload Platform Transactions CSV", type="csv")
with col2:
    bank_file = st.file_uploader("Upload Bank Settlements CSV", type="csv")

if plat_file and bank_file:
    try:
        # Load and validate data
        plat = pd.read_csv(plat_file, parse_dates=['timestamp'])
        bank = pd.read_csv(bank_file, parse_dates=['settlement_date'])
        
        required_plat = {'txn_id', 'amount', 'timestamp', 'type', 'original_txn_id'}
        required_bank = {'txn_id', 'amount', 'settlement_date'}
        
        if not required_plat.issubset(plat.columns):
            st.error(f"Platform file missing columns: {required_plat - set(plat.columns)}")
            st.stop()
        if not required_bank.issubset(bank.columns):
            st.error(f"Bank file missing columns: {required_bank - set(bank.columns)}")
            st.stop()
        
        plat['amount'] = plat['amount'].astype(float)
        bank['amount'] = bank['amount'].astype(float)
        
        st.success("Files loaded successfully")
        
        # Detect anomalies
        next_month = detect_next_month_settlement(plat, bank)
        total_diff, mismatches = detect_rounding_differences(plat, bank)
        plat_dups = detect_duplicates_platform(plat)
        bank_dups = detect_duplicates_bank(bank)
        orphans = detect_orphan_refunds(plat)
        
        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Next‑Month Settlements", len(next_month))
        col2.metric("Rounding Mismatches", len(mismatches))
        col3.metric("Platform Duplicates", len(plat_dups))
        col4.metric("Orphan Refunds", len(orphans))
        
        # Bar chart summary
        summary_df = pd.DataFrame({
            "Anomaly Type": ["Next‑Month Settlement", "Rounding Mismatch", "Platform Duplicate", "Bank Duplicate", "Orphan Refund"],
            "Count": [len(next_month), len(mismatches), len(plat_dups), len(bank_dups), len(orphans)]
        })
        fig = px.bar(summary_df, x="Anomaly Type", y="Count", title="Reconciliation Anomalies Summary", color="Anomaly Type")
        st.plotly_chart(fig, use_container_width=True)
        
        # Tabs for detailed views
        tab1, tab2, tab3, tab4 = st.tabs(["📅 Next‑Month Settlement", "🔢 Rounding Differences", "🔄 Duplicates", "💔 Orphan Refunds"])
        
        with tab1:
            if len(next_month) > 0:
                st.dataframe(next_month, use_container_width=True)
                csv = next_month.to_csv(index=False).encode('utf-8')
                st.download_button("Download CSV", csv, "next_month_settlements.csv", "text/csv")
            else:
                st.info("No transactions settled in a different month.")
        
        with tab2:
            st.metric("Total Platform Sum", f"${plat['amount'].sum():,.2f}")
            st.metric("Total Bank Sum", f"${bank['amount'].sum():,.2f}")
            st.metric("Difference", f"${total_diff:.2f}", delta=f"{total_diff:.2f}")
            if len(mismatches) > 0:
                st.subheader("Per‑transaction Mismatches")
                st.dataframe(mismatches, use_container_width=True)
                csv = mismatches.to_csv(index=False).encode('utf-8')
                st.download_button("Download CSV", csv, "rounding_mismatches.csv", "text/csv")
            else:
                st.success("No per‑transaction rounding mismatches.")
        
        with tab3:
            st.subheader("Platform Duplicates")
            if len(plat_dups) > 0:
                st.dataframe(plat_dups[['txn_id', 'amount', 'timestamp']], use_container_width=True)
                csv = plat_dups.to_csv(index=False).encode('utf-8')
                st.download_button("Download CSV", csv, "platform_duplicates.csv", "text/csv")
            else:
                st.success("No duplicate txn_id in platform data.")
            
            st.subheader("Bank Duplicates")
            if len(bank_dups) > 0:
                st.dataframe(bank_dups[['txn_id', 'amount', 'settlement_date']], use_container_width=True)
                csv = bank_dups.to_csv(index=False).encode('utf-8')
                st.download_button("Download CSV", csv, "bank_duplicates.csv", "text/csv")
            else:
                st.success("No duplicate txn_id in bank data.")
        
        with tab4:
            if len(orphans) > 0:
                st.dataframe(orphans, use_container_width=True)
                csv = orphans.to_csv(index=False).encode('utf-8')
                st.download_button("Download CSV", csv, "orphan_refunds.csv", "text/csv")
            else:
                st.info("No orphan refunds found.")
    
    except Exception as e:
        st.error(f"Error processing files: {str(e)}")
else:
    st.info("Please upload both CSV files to begin reconciliation.")