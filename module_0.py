import pandas as pd
import QuantLib as ql
from pykrx import stock
from datetime import datetime
import pytz
import streamlit as st


def get_latest_business_day(target_date=None):
    """
    Uses QuantLib to find the valid trading day (Seoul Time).
    """
    calendar = ql.SouthKorea()

    # Force KST Timezone logic (Crucial for Cloud Servers)
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

    # Prevent Future Dates
    if date_ql > today_ql:
        date_ql = today_ql

    return calendar.adjust(date_ql, ql.Preceding)


@st.cache_data(ttl=3600)
def fetch_krx_snapshot(target_date):
    """
    Fetches Price + Market Cap + Names.
    Includes simple retry logic for weekends/holidays.
    """
    current_date_ql = get_latest_business_day(target_date)
    calendar = ql.SouthKorea()
    max_retries = 5  # Try up to 5 days back

    for attempt in range(max_retries):
        date_str = f"{current_date_ql.year()}{current_date_ql.month():02d}{current_date_ql.dayOfMonth():02d}"
        print(f"🔄 Attempt {attempt + 1}: Fetching KRX Data for [{date_str}]...")

        try:
            # 1. Fetch Price (Standard OHLCV)
            df_price = stock.get_market_ohlcv(date_str, market="ALL")

            # Simple check for empty data
            if df_price is None or df_price.empty:
                print(f"   ⚠️ Data empty for {date_str}. Rewinding...")
                current_date_ql = calendar.adjust(current_date_ql - 1, ql.Preceding)
                continue

            # --- CLEAN FIX: English Header Support ---
            # If server returns English headers, rename them to Korean standard
            rename_map = {
                'Open': '시가', 'High': '고가', 'Low': '저가', 'Close': '종가',
                'Volume': '거래량', 'Amount': '거래대금', 'Fluctuation': '등락률', 'Change': '등락률'
            }
            df_price.rename(columns=rename_map, inplace=True)
            # -----------------------------------------

            # 2. Fetch Cap
            df_cap = stock.get_market_cap(date_str, market="ALL")

            # --- CLEAN FIX: English Header Support ---
            cap_map = {'Marcap': '시가총액', 'Shares': '상장주식수', 'Market Cap': '시가총액'}
            df_cap.rename(columns=cap_map, inplace=True)
            # -----------------------------------------

            # 3. Merge
            df_merged = pd.merge(df_price, df_cap, left_index=True, right_index=True, how='inner')

            if df_merged.empty:
                current_date_ql = calendar.adjust(current_date_ql - 1, ql.Preceding)
                continue

            df_merged = df_merged.reset_index()

            # Rename Ticker column (Handle variations)
            cols = df_merged.columns
            if '티커' in cols:
                df_merged.rename(columns={'티커': 'Code'}, inplace=True)
            elif 'Ticker' in cols:
                df_merged.rename(columns={'Ticker': 'Code'}, inplace=True)
            elif 'index' in cols:
                df_merged.rename(columns={'index': 'Code'}, inplace=True)

            # 4. Names
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

            print(f"✅ Success! Fetched {len(df_merged)} tickers.")
            return df_merged

        except Exception as e:
            print(f"❌ Error on {date_str}: {e}")
            current_date_ql = calendar.adjust(current_date_ql - 1, ql.Preceding)

    print("❌ Failed to fetch data after multiple attempts.")
    return pd.DataFrame()


if __name__ == "__main__":
    pass