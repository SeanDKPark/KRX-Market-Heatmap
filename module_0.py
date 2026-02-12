import pandas as pd
import QuantLib as ql
from pykrx import stock
from datetime import datetime, timedelta
import pytz
import streamlit as st


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


# REMOVE CACHE FOR DEBUGGING
# @st.cache_data(ttl=3600)
def fetch_krx_snapshot(target_date):
    current_date_ql = get_latest_business_day(target_date)
    calendar = ql.SouthKorea()
    max_retries = 3

    st.info("🔍 Starting Data Fetch Process (Diagnostic Mode)...")

    for attempt in range(max_retries):
        date_str = f"{current_date_ql.year()}{current_date_ql.month():02d}{current_date_ql.dayOfMonth():02d}"
        st.write(f"🔄 Attempt {attempt + 1}: Trying date **{date_str}**...")

        try:
            # 1. Fetch Price
            st.caption(f"   ... Requesting OHLCV for {date_str}...")
            df_price = stock.get_market_ohlcv(date_str, market="ALL")

            if df_price is None or df_price.empty:
                st.warning(f"   ⚠️ Result empty for {date_str}.")
                current_date_ql = calendar.adjust(current_date_ql - 1, ql.Preceding)
                continue

            # --- DIAGNOSTIC PRINT ---
            st.write(f"   📊 Price Columns Received: `{list(df_price.columns)}`")
            # ------------------------

            # 2. Fetch Cap
            st.caption(f"   ... Requesting Market Cap for {date_str}...")
            df_cap = stock.get_market_cap(date_str, market="ALL")

            # --- DIAGNOSTIC PRINT ---
            st.write(f"   📊 Cap Columns Received: `{list(df_cap.columns)}`")
            # ------------------------

            # 3. Standardize Columns (Adaptive)
            # We construct a rename map based on what we ACTUALLY have
            price_map = {
                '시가': 'Open', '고가': 'High', '저가': 'Low', '종가': 'Close',
                '거래량': 'Volume', '거래대금': 'Amount', '등락률': 'ChagesRatio'
            }
            # Only rename columns that exist
            actual_rename = {k: v for k, v in price_map.items() if k in df_price.columns}
            df_price.rename(columns=actual_rename, inplace=True)

            cap_map = {'시가총액': 'Marcap', '상장주식수': 'Shares'}
            actual_cap_rename = {k: v for k, v in cap_map.items() if k in df_cap.columns}
            df_cap.rename(columns=actual_cap_rename, inplace=True)

            # 4. Merge
            # Check for common index or columns
            df_merged = pd.merge(df_price, df_cap, left_index=True, right_index=True, how='inner')

            if df_merged.empty:
                st.error(f"   ⚠️ Merge resulted in empty dataframe!")
                current_date_ql = calendar.adjust(current_date_ql - 1, ql.Preceding)
                continue

            df_merged = df_merged.reset_index()

            # Rename Ticker column (It might be index name or a column)
            # If '티커' is in columns, rename it. If it was the index, reset_index made it 'index' or 'Code' or 'Ticker'
            # Let's check columns after reset
            cols = df_merged.columns
            if '티커' in cols:
                df_merged.rename(columns={'티커': 'Code'}, inplace=True)
            elif 'Ticker' in cols:
                df_merged.rename(columns={'Ticker': 'Code'}, inplace=True)
            elif 'index' in cols:
                df_merged.rename(columns={'index': 'Code'}, inplace=True)  # Fallback if index had no name

            # 5. Names
            st.caption("   ... Mapping Names...")
            df_merged['Name'] = df_merged['Code'].apply(lambda x: stock.get_market_ticker_name(x))

            # 6. Metadata & Market
            df_merged['Snapshot_Date'] = date_str

            # Market division logic (try/except block to be safe)
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
            st.error(f"❌ CRITICAL ERROR on {date_str}: {e}")
            # Print full dataframe columns to see what went wrong
            try:
                if 'df_price' in locals(): st.write(f"Final Price Cols: {df_price.columns}")
            except:
                pass

            current_date_ql = calendar.adjust(current_date_ql - 1, ql.Preceding)

    st.error("❌ All attempts failed.")
    return pd.DataFrame()


if __name__ == "__main__":
    pass