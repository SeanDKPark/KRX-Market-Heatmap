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
    # Reset Log
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

        df_price = pd.DataFrame()

        # --- STRATEGY 1: get_market_ohlcv (Standard) ---
        try:
            log_message("Strategy 1: Requesting OHLCV...")
            df_price = stock.get_market_ohlcv(date_str, market="ALL")
        except Exception as e:
            log_message(f"Strategy 1 Failed: {e}")
            df_price = pd.DataFrame()  # Reset

        # --- STRATEGY 2: get_market_price_change_by_ticker (Alternative) ---
        if df_price.empty:
            try:
                log_message("Strategy 2: Requesting Price Change By Ticker...")
                # Fetch start_date=end_date to get snapshot
                df_price = stock.get_market_price_change_by_ticker(date_str, date_str)
            except Exception as e:
                log_message(f"Strategy 2 Failed: {e}")
                df_price = pd.DataFrame()

        # --- STRATEGY 3: get_market_cap (Survival - Close/Cap only) ---
        if df_price.empty:
            try:
                log_message("Strategy 3: Requesting Market Cap Only (Survival Mode)...")
                # This usually works because it has simpler columns
                df_price = stock.get_market_cap(date_str, market="ALL")
                # This result lacks 'Open/High/Low' and maybe 'Change', but has 'Close'
            except Exception as e:
                log_message(f"Strategy 3 Failed: {e}")

        # --- CHECK RESULT ---
        if df_price is None or df_price.empty:
            st.warning(f"   ⚠️ No data found for {date_str} using any strategy.")
            current_date_ql = calendar.adjust(current_date_ql - 1, ql.Preceding)
            continue

        # --- SUCCESS! NORMALIZE COLUMNS ---
        log_message(f"Data received! Columns: {list(df_price.columns)}")

        # Robust Rename Map (Covers all strategies)
        rename_map = {
            '시가': 'Open', '고가': 'High', '저가': 'Low', '종가': 'Close',
            '거래량': 'Volume', '거래대금': 'Amount', '등락률': 'ChagesRatio',
            '대비': 'Change', '시가총액': 'Marcap', '상장주식수': 'Shares',
            # English Fallbacks
            'Open': 'Open', 'High': 'High', 'Low': 'Low', 'Close': 'Close',
            'Volume': 'Volume', 'Amount': 'Amount', 'Fluctuation': 'ChagesRatio',
            'Change': 'ChagesRatio', 'Market Cap': 'Marcap'
        }
        actual_rename = {k: v for k, v in rename_map.items() if k in df_price.columns}
        df_price.rename(columns=actual_rename, inplace=True)

        # Ensure we have essential columns
        if 'ChagesRatio' not in df_price.columns:
            df_price['ChagesRatio'] = 0.0  # Fill 0 if missing (Strategy 3)
        if 'Marcap' not in df_price.columns:
            # If Strategy 1 or 2 succeeded, we still need Cap. Fetch it now.
            try:
                df_cap = stock.get_market_cap(date_str, market="ALL")
                cap_map = {'시가총액': 'Marcap', '상장주식수': 'Shares', 'Market Cap': 'Marcap'}
                df_cap.rename(columns={k: v for k, v in cap_map.items() if k in df_cap.columns}, inplace=True)
                # Merge
                df_price = pd.merge(df_price, df_cap[['Marcap', 'Shares']], left_index=True, right_index=True,
                                    how='left')
            except:
                df_price['Marcap'] = 0  # Final fallback

        # Reset Index (Ticker)
        df_final = df_price.reset_index()
        cols = df_final.columns
        if '티커' in cols:
            df_final.rename(columns={'티커': 'Code'}, inplace=True)
        elif 'Ticker' in cols:
            df_final.rename(columns={'Ticker': 'Code'}, inplace=True)
        elif 'index' in cols:
            df_final.rename(columns={'index': 'Code'}, inplace=True)

        # Names
        log_message("Mapping Names...")
        df_final['Name'] = df_final['Code'].apply(lambda x: stock.get_market_ticker_name(x))
        df_final['Snapshot_Date'] = date_str

        # Market
        try:
            kospi_set = set(stock.get_market_ticker_list(date_str, market="KOSPI"))
            kosdaq_set = set(stock.get_market_ticker_list(date_str, market="KOSDAQ"))

            def assign_market(code):
                if code in kospi_set: return "KOSPI"
                if code in kosdaq_set: return "KOSDAQ"
                return "KONEX"

            df_final['Market'] = df_final['Code'].apply(assign_market)
        except:
            df_final['Market'] = "Unknown"

        st.balloons()
        return df_final

    st.error("❌ All attempts failed.")
    # Show Log
    with st.expander("View Debug Log"):
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            st.text(f.read())

    return pd.DataFrame()


if __name__ == "__main__":
    pass