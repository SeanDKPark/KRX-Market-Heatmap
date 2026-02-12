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

            # --- FIX: Standardize Columns Immediately ---
            # Map known Korean/English variations to standard English
            # We check what columns we actually got
            cols = df_price.columns
            rename_map = {}
            if '시가' in cols: rename_map['시가'] = 'Open'
            if '고가' in cols: rename_map['고가'] = 'High'
            if '저가' in cols: rename_map['저가'] = 'Low'
            if '종가' in cols: rename_map['종가'] = 'Close'
            if '거래량' in cols: rename_map['거래량'] = 'Volume'
            if '거래대금' in cols: rename_map['거래대금'] = 'Amount'
            if '등락률' in cols: rename_map['등락률'] = 'ChagesRatio'

            if rename_map:
                df_price.rename(columns=rename_map, inplace=True)
            # ---------------------------------------------

            st.success(f"   ✅ Got Price Data! ({len(df_price)} rows)")

            # 2. Fetch Cap
            st.caption(f"   ... Requesting Market Cap for {date_str}...")
            df_cap = stock.get_market_cap(date_str, market="ALL")

            # --- FIX: Standardize Cap Columns ---
            cols_cap = df_cap.columns
            cap_map = {}
            if '시가총액' in cols_cap: cap_map['시가총액'] = 'Marcap'
            if '상장주식수' in cols_cap: cap_map['상장주식수'] = 'Shares'
            if cap_map:
                df_cap.rename(columns=cap_map, inplace=True)
            # ------------------------------------

            # 3. Merge
            # Use English keys if possible, or fallback
            df_merged = pd.merge(df_price, df_cap, left_index=True, right_index=True, how='inner')

            if df_merged.empty:
                st.error(f"   ⚠️ Merge resulted in empty dataframe!")
                current_date_ql = calendar.adjust(current_date_ql - 1, ql.Preceding)
                continue

            df_merged = df_merged.reset_index()
            # Rename 'Ticker' column which might be '티커' or 'Ticker'
            if '티커' in df_merged.columns:
                df_merged.rename(columns={'티커': 'Code'}, inplace=True)
            elif 'Ticker' in df_merged.columns:
                df_merged.rename(columns={'Ticker': 'Code'}, inplace=True)

            # 4. Names
            st.caption("   ... Mapping Names...")
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
            except:
                df_merged['Market'] = "Unknown"

            st.balloons()
            return df_merged

        except Exception as e:
            st.error(f"❌ CRITICAL ERROR on {date_str}: {e}")
            # print full stack trace to logs if needed
            print(e)
            current_date_ql = calendar.adjust(current_date_ql - 1, ql.Preceding)

    st.error("❌ All attempts failed.")
    return pd.DataFrame()


if __name__ == "__main__":
    pass