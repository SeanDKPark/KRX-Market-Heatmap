import pandas as pd
import QuantLib as ql
from pykrx import stock
from datetime import datetime
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


@st.cache_data(ttl=3600)
def fetch_krx_snapshot(target_date):
    current_date_ql = get_latest_business_day(target_date)
    calendar = ql.SouthKorea()
    max_retries = 3

    st.info("🔍 Starting Data Fetch Process...")

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

            # --- DEBUG: Print Raw Columns ---
            st.write(f"   📊 Raw Columns: {list(df_price.columns)}")

            # --- ROBUST RENAMING ---
            # We map BOTH Korean and English variations to our standard names.
            # This ensures it works regardless of server locale.
            rename_map = {
                # Korean to English
                '시가': 'Open', '고가': 'High', '저가': 'Low', '종가': 'Close',
                '거래량': 'Volume', '거래대금': 'Amount', '등락률': 'ChagesRatio',
                # English to English (Just in case they are already English but we want to be sure)
                'Open': 'Open', 'High': 'High', 'Low': 'Low', 'Close': 'Close',
                'Volume': 'Volume', 'Amount': 'Amount', 'Fluctuation': 'ChagesRatio', 'Change': 'ChagesRatio'
            }
            df_price.rename(columns=rename_map, inplace=True)

            # Ensure 'ChagesRatio' exists (sometimes missing if price unchanged? unlikely but safe)
            if 'ChagesRatio' not in df_price.columns:
                df_price['ChagesRatio'] = 0.0

            st.success(f"   ✅ Got Price Data! ({len(df_price)} rows)")

            # 2. Fetch Cap
            st.caption(f"   ... Requesting Market Cap for {date_str}...")
            df_cap = stock.get_market_cap(date_str, market="ALL")

            # Rename Cap Columns
            cap_map = {
                '시가총액': 'Marcap', '상장주식수': 'Shares',
                'Marcap': 'Marcap', 'Shares': 'Shares',
                'Market Cap': 'Marcap'
            }
            df_cap.rename(columns=cap_map, inplace=True)

            # 3. Merge
            df_merged = pd.merge(df_price, df_cap, left_index=True, right_index=True, how='inner')

            if df_merged.empty:
                st.error(f"   ⚠️ Merge resulted in empty dataframe!")
                current_date_ql = calendar.adjust(current_date_ql - 1, ql.Preceding)
                continue

            df_merged = df_merged.reset_index()

            # Rename Ticker (Index)
            # It usually comes as '티커' or 'Ticker' or just 'index'
            cols = df_merged.columns
            if '티커' in cols:
                df_merged.rename(columns={'티커': 'Code'}, inplace=True)
            elif 'Ticker' in cols:
                df_merged.rename(columns={'Ticker': 'Code'}, inplace=True)
            elif 'index' in cols:
                df_merged.rename(columns={'index': 'Code'}, inplace=True)

            # 4. Names & Metadata
            st.caption("   ... Mapping Names...")
            df_merged['Name'] = df_merged['Code'].apply(lambda x: stock.get_market_ticker_name(x))
            df_merged['Snapshot_Date'] = date_str

            # Market Logic
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
            current_date_ql = calendar.adjust(current_date_ql - 1, ql.Preceding)

    st.error("❌ All attempts failed.")
    return pd.DataFrame()


if __name__ == "__main__":
    pass