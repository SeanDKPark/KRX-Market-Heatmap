import pandas as pd
import QuantLib as ql
from pykrx import stock
from datetime import datetime, timedelta
import pytz
import streamlit as st


def get_latest_business_day(target_date=None):
    """
    Uses QuantLib to find the valid trading day.
    """
    calendar = ql.SouthKorea()

    # Force KST Timezone logic
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

    # 1. Prevent Future Dates
    if date_ql > today_ql:
        print(f"⚠️ Future date detected. Snapping to Today.")
        date_ql = today_ql

    # 2. Adjust for Weekend/Holiday (Initial Check)
    valid_date_ql = calendar.adjust(date_ql, ql.Preceding)

    return valid_date_ql


@st.cache_data(ttl=3600)  # Cache for 1 hour to speed up for other users
def fetch_krx_snapshot(target_date):
    """
    Fetches Price + Market Cap + Names with robust retry logic.
    """
    # 1. Get Initial Target Date
    current_date_ql = get_latest_business_day(target_date)
    calendar = ql.SouthKorea()

    max_retries = 5

    for attempt in range(max_retries):
        # Convert QL Date to String "YYYYMMDD"
        date_str = f"{current_date_ql.year()}{current_date_ql.month():02d}{current_date_ql.dayOfMonth():02d}"
        print(f"🔄 Attempt {attempt + 1}: Fetching KRX Data for [{date_str}]...")

        try:
            # 2. Fetch Price (OHLCV)
            # market="ALL" fetches KOSPI + KOSDAQ + KONEX
            df_price = stock.get_market_ohlcv(date_str, market="ALL")

            # CRITICAL CHECK: Is the dataframe empty?
            if df_price is None or df_price.empty:
                print(f"   ⚠️ Data empty for {date_str}. Rewinding 1 business day...")
                current_date_ql = calendar.adjust(current_date_ql - 1, ql.Preceding)
                continue

            # 3. Fetch Cap (Shares, Marcap)
            df_cap = stock.get_market_cap(date_str, market="ALL")

            # 4. Merge
            # inner join ensures we only keep stocks that have both price and cap data
            df_merged = pd.merge(df_price, df_cap, left_index=True, right_index=True, how='inner')

            if df_merged.empty:
                print(f"   ⚠️ Merged data empty for {date_str}. Rewinding...")
                current_date_ql = calendar.adjust(current_date_ql - 1, ql.Preceding)
                continue

            df_merged = df_merged.reset_index()
            df_merged.rename(columns={'티커': 'Code'}, inplace=True)

            # 5. Add Names
            # This can be slow, but necessary.
            # Optimization: Fetch ticker list with names if possible, but pykrx separates them.
            # We stick to the map for accuracy.
            print("   🏷️ Mapping Tickers to Names...")
            df_merged['Name'] = df_merged['Code'].apply(lambda x: stock.get_market_ticker_name(x))

            # 6. Metadata
            df_merged['Snapshot_Date'] = date_str

            # 7. Market Division
            kospi_set = set(stock.get_market_ticker_list(date_str, market="KOSPI"))
            kosdaq_set = set(stock.get_market_ticker_list(date_str, market="KOSDAQ"))

            def assign_market(code):
                if code in kospi_set: return "KOSPI"
                if code in kosdaq_set: return "KOSDAQ"
                return "KONEX"

            df_merged['Market'] = df_merged['Code'].apply(assign_market)

            print(f"✅ Success! Fetched {len(df_merged)} tickers from {date_str}.")
            return df_merged

        except Exception as e:
            print(f"❌ Error on {date_str}: {e}")
            # If a crash happens (e.g. network error), try previous day
            current_date_ql = calendar.adjust(current_date_ql - 1, ql.Preceding)

    print("❌ Failed to fetch data after multiple attempts.")
    return pd.DataFrame()  # Return empty if all fails


if __name__ == "__main__":
    # Test Block
    df = fetch_krx_snapshot("20260212")
    print(df.head())