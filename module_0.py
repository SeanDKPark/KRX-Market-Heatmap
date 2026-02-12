import pandas as pd
import QuantLib as ql
from pykrx import stock
from pykrx.website.krx.market.ticker import Ticker
from datetime import datetime
import pytz
import streamlit as st
import os

# --- LOGGING ---
LOG_FILE = "debug_log.txt"


def log_message(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {msg}\n")


# --- MONKEY PATCHING PYKRX ---
# We are rewriting the internal valid column mapper to handle English headers
# This runs immediately when module_0 is imported.
try:
    from pykrx.website.comm.util import dataframe_empty_handler
    # We can't easily patch the deep internals safely without copying 100 lines.
    # Instead, we will use a 'cleaner' scraper approach.
except:
    pass


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

    for attempt in range(max_retries):
        date_str = f"{current_date_ql.year()}{current_date_ql.month():02d}{current_date_ql.dayOfMonth():02d}"
        st.write(f"🔄 Attempt {attempt + 1}: Trying date **{date_str}**...")
        log_message(f"Attempt {attempt + 1}: {date_str}")

        try:
            # STRATEGY: Use the rawest possible fetch to avoid PyKRX internal renaming crashes.
            # get_market_ohlcv_by_ticker IS usually safer than get_market_ohlcv(date)
            # because it returns a simpler structure.

            # 1. Fetch Price
            log_message("Fetching OHLCV...")
            # Note: We use the 'by_ticker' variation which downloads the whole market for one day
            df_price = stock.get_market_ohlcv(date_str, market="ALL")

            # --- CRITICAL FIX ---
            # If pykrx didn't crash but returned English columns, we normalize them here.
            # If pykrx CRASHED inside the library, we catch it below.

            # Normalize Price Columns
            price_map = {
                '시가': 'Open', '고가': 'High', '저가': 'Low', '종가': 'Close',
                '거래량': 'Volume', '거래대금': 'Amount', '등락률': 'ChagesRatio',
                'Open': 'Open', 'High': 'High', 'Low': 'Low', 'Close': 'Close',
                'Volume': 'Volume', 'Amount': 'Amount', 'Fluctuation': 'ChagesRatio', 'Change': 'ChagesRatio'
            }
            df_price.rename(columns=price_map, inplace=True)

            # 2. Fetch Cap
            log_message("Fetching Cap...")
            df_cap = stock.get_market_cap(date_str, market="ALL")

            cap_map = {
                '시가총액': 'Marcap', '상장주식수': 'Shares',
                'Marcap': 'Marcap', 'Shares': 'Shares', 'Market Cap': 'Marcap'
            }
            df_cap.rename(columns=cap_map, inplace=True)

            # 3. Merge
            df_merged = pd.merge(df_price, df_cap, left_index=True, right_index=True, how='inner')

            if df_merged.empty:
                log_message("Merge Empty.")
                current_date_ql = calendar.adjust(current_date_ql - 1, ql.Preceding)
                continue

            df_merged = df_merged.reset_index()
            cols = df_merged.columns
            if '티커' in cols:
                df_merged.rename(columns={'티커': 'Code'}, inplace=True)
            elif 'Ticker' in cols:
                df_merged.rename(columns={'Ticker': 'Code'}, inplace=True)
            elif 'index' in cols:
                df_merged.rename(columns={'index': 'Code'}, inplace=True)

            # 4. Names
            log_message("Mapping Names...")
            df_merged['Name'] = df_merged['Code'].apply(lambda x: stock.get_market_ticker_name(x))
            df_merged['Snapshot_Date'] = date_str

            # 5. Market
            try:
                kospi_set = set(stock.get_market_ticker_list(date_str, market="KOSPI"))
                kosdaq_set = set(stock.get_market_ticker_list(date_str, market="KOSDAQ"))

                def assign_market(code):
                    if code in kospi_set: return "KOSPI"
                    if code in kosdaq_set: return "KOSDAQ"
                    return "KONEX"

                df_merged['Market'] = df_merged['Code'].apply(assign_market)
            except:
                df_merged['Market'] = "Unknown"

            st.balloons()
            return df_merged

        except Exception as e:
            # THIS IS WHERE WE CATCH THE LIBRARY CRASH
            error_msg = str(e)
            log_message(f"CRASH: {error_msg}")

            # If the crash is explicitly about missing columns, it means
            # PyKRX fetched data successfully but failed to process headers.
            # We can try to use a raw Naver fetch if this fails, but that's complex.
            # For now, we fallback to previous day.

            current_date_ql = calendar.adjust(current_date_ql - 1, ql.Preceding)

    st.error("❌ All attempts failed. Check 'View Debug Log' below.")
    with st.expander("View Debug Log"):
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            st.text(f.read())

    return pd.DataFrame()


if __name__ == "__main__":
    pass