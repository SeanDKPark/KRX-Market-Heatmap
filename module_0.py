import pandas as pd
import QuantLib as ql
from pykrx import stock
from datetime import datetime
import pytz
import streamlit as st
import os

# --- LOGGING SETUP ---
LOG_FILE = "debug_log.txt"


def log_message(msg):
    """Writes a message to the log file and prints to console."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    entry = f"[{timestamp}] {msg}"
    print(entry)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(entry + "\n")


def get_latest_business_day(target_date=None):
    calendar = ql.SouthKorea()
    kst = pytz.timezone('Asia/Seoul')
    now_kst = datetime.now(kst)
    today_ql = ql.Date(now_kst.day, now_kst.month, now_kst.year)

    if target_date is None:
        date_ql = today_ql
    elif isinstance(target_date, str):
        d = datetime.strptime(target_date, "%Y%m%d")
        date_ql = ql.Date(d.day, d.month, d.year)
    else:
        date_ql = ql.Date(target_date.day, target_date.month, target_date.year)

    if date_ql > today_ql:
        date_ql = today_ql

    return calendar.adjust(date_ql, ql.Preceding)


@st.cache_data(ttl=3600)
def fetch_krx_snapshot(target_date):
    # Reset Log File
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        f.write("--- NEW FETCH SESSION ---\n")

    current_date_ql = get_latest_business_day(target_date)
    calendar = ql.SouthKorea()
    max_retries = 3

    log_message(f"Starting fetch for target date: {target_date}")

    for attempt in range(max_retries):
        date_str = f"{current_date_ql.year()}{current_date_ql.month():02d}{current_date_ql.dayOfMonth():02d}"
        log_message(f"Attempt {attempt + 1}: Trying {date_str}...")

        st.write(f"🔄 Attempt {attempt + 1}: Trying date **{date_str}**...")

        try:
            # 1. Fetch Price
            log_message("Requesting OHLCV...")
            df_price = stock.get_market_ohlcv(date_str, market="ALL")

            if df_price is None or df_price.empty:
                log_message(f"Result empty for {date_str}.")
                st.warning(f"   ⚠️ Result empty for {date_str}.")
                current_date_ql = calendar.adjust(current_date_ql - 1, ql.Preceding)
                continue

            # --- DIAGNOSTIC: Log Raw Columns ---
            raw_cols = list(df_price.columns)
            log_message(f"Raw Price Columns Received: {raw_cols}")
            st.write(f"   📊 Columns: `{raw_cols}`")

            # --- ROBUST RENAMING (The Fix) ---
            # We map everything to standard English keys
            rename_map = {
                '시가': 'Open', '고가': 'High', '저가': 'Low', '종가': 'Close',
                '거래량': 'Volume', '거래대금': 'Amount', '등락률': 'ChagesRatio',
                'Open': 'Open', 'High': 'High', 'Low': 'Low', 'Close': 'Close',
                'Volume': 'Volume', 'Amount': 'Amount', 'Fluctuation': 'ChagesRatio', 'Change': 'ChagesRatio'
            }
            # Only rename columns that actually exist to avoid KeyError
            actual_rename = {k: v for k, v in rename_map.items() if k in df_price.columns}
            df_price.rename(columns=actual_rename, inplace=True)

            log_message(f"Renamed Columns: {list(df_price.columns)}")

            # 2. Fetch Cap
            log_message("Requesting Market Cap...")
            df_cap = stock.get_market_cap(date_str, market="ALL")

            log_message(f"Raw Cap Columns: {list(df_cap.columns)}")

            cap_map = {
                '시가총액': 'Marcap', '상장주식수': 'Shares',
                'Marcap': 'Marcap', 'Shares': 'Shares', 'Market Cap': 'Marcap'
            }
            actual_cap_rename = {k: v for k, v in cap_map.items() if k in df_cap.columns}
            df_cap.rename(columns=actual_cap_rename, inplace=True)

            # 3. Merge
            log_message("Merging Price and Cap...")
            df_merged = pd.merge(df_price, df_cap, left_index=True, right_index=True, how='inner')

            if df_merged.empty:
                log_message("Merge resulted in empty dataframe.")
                st.error(f"   ⚠️ Merge resulted in empty dataframe!")
                current_date_ql = calendar.adjust(current_date_ql - 1, ql.Preceding)
                continue

            df_merged = df_merged.reset_index()

            # Rename Ticker
            cols = df_merged.columns
            if '티커' in cols:
                df_merged.rename(columns={'티커': 'Code'}, inplace=True)
            elif 'Ticker' in cols:
                df_merged.rename(columns={'Ticker': 'Code'}, inplace=True)
            elif 'index' in cols:
                df_merged.rename(columns={'index': 'Code'}, inplace=True)

            log_message(f"Merged Columns: {list(df_merged.columns)}")

            # 4. Names
            log_message("Mapping Names...")
            df_merged['Name'] = df_merged['Code'].apply(lambda x: stock.get_market_ticker_name(x))

            # 5. Metadata
            df_merged['Snapshot_Date'] = date_str

            # 6. Market Division
            try:
                kospi_set = set(stock.get_market_ticker_list(date_str, market="KOSPI"))
                kosdaq_set = set(stock.get_market_ticker_list(date_str, market="KOSDAQ"))

                def assign_market(code):
                    if code in kospi_set: return "KOSPI"
                    if code in kosdaq_set: return "KOSDAQ"
                    return "KONEX"

                df_merged['Market'] = df_merged['Code'].apply(assign_market)
            except Exception as e:
                log_message(f"Market assignment error: {e}")
                df_merged['Market'] = "Unknown"

            log_message(f"Success! {len(df_merged)} rows.")
            st.balloons()

            # Add View Log Button (Visible only on success)
            with st.expander("View Debug Log"):
                with open(LOG_FILE, "r", encoding="utf-8") as f:
                    st.text(f.read())

            return df_merged

        except Exception as e:
            log_message(f"CRITICAL ERROR: {e}")
            st.error(f"❌ CRITICAL ERROR on {date_str}: {e}")

            # Show log immediately on error
            with st.expander("View Debug Log (Error Trace)"):
                with open(LOG_FILE, "r", encoding="utf-8") as f:
                    st.text(f.read())

            current_date_ql = calendar.adjust(current_date_ql - 1, ql.Preceding)

    log_message("All attempts failed.")
    st.error("❌ All attempts failed.")
    return pd.DataFrame()


if __name__ == "__main__":
    pass