import pandas as pd
import QuantLib as ql
from pykrx import stock
from datetime import datetime
import pytz

def get_latest_business_day(target_date=None):
    """
    Uses QuantLib to find the valid trading day.
    """
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
        print(f"⚠️ Future date detected. Snapping to Today.")
        date_ql = today_ql

    valid_date_ql = calendar.adjust(date_ql, ql.Preceding)
    valid_date_str = f"{valid_date_ql.year()}{valid_date_ql.month():02d}{valid_date_ql.dayOfMonth():02d}"

    return valid_date_str


def fetch_krx_snapshot(target_date):
    """
    Fetches Price + Market Cap + Shares + NAMES for the valid business day.
    """
    # 1. Get Robust Date
    date_str = get_latest_business_day(target_date)
    print(f"🔄 Fetching KRX Data for [{date_str}]...")

    try:
        # 2. Fetch Price (OHLCV)
        df_price = stock.get_market_ohlcv(date_str, market="ALL")

        # 3. Fetch Fundamental/Cap (Shares, Marcap)
        df_cap = stock.get_market_cap(date_str, market="ALL")

        # 4. Merge Data
        df_merged = pd.merge(df_price, df_cap, left_index=True, right_index=True, how='inner')
        df_merged = df_merged.reset_index()
        df_merged.rename(columns={'티커': 'Code'}, inplace=True)

        # --- NEW STEP: Add Stock Names ---
        print("   🏷️ Mapping Tickers to Names (This may take a few seconds)...")
        # PyKRX doesn't give names by default in OHLCV, so we look them up.
        # Ideally, we fetch the ticker list for that date which might contain names,
        # but PyKRX structure separates them. The fastest way is to map:

        # Get all tickers for that date (KOSPI + KOSDAQ + KONEX) to ensure coverage
        # Note: stock.get_market_ticker_name(code) gets the CURRENT name.
        # For historical names, it's trickier, but current name is usually acceptable.

        df_merged['Name'] = df_merged['Code'].apply(lambda x: stock.get_market_ticker_name(x))

        # ---------------------------------

        # 5. Add Metadata
        df_merged['Snapshot_Date'] = date_str

        # 6. Add Market Division (KOSPI/KOSDAQ)
        kospi_set = set(stock.get_market_ticker_list(date_str, market="KOSPI"))
        kosdaq_set = set(stock.get_market_ticker_list(date_str, market="KOSDAQ"))

        def assign_market(code):
            if code in kospi_set: return "KOSPI"
            if code in kosdaq_set: return "KOSDAQ"
            return "KONEX"

        df_merged['Market'] = df_merged['Code'].apply(assign_market)

        print(f"✅ Success. Fetched {len(df_merged)} tickers.")
        return df_merged

    except Exception as e:
        print(f"❌ Error fetching data: {e}")
        return pd.DataFrame()

# module_0.py (Bottom part)
if __name__ == "__main__":
    # This line is ONLY executed when you run "python module_0.py"
    df = fetch_krx_snapshot("20260210")
    print(df[['Code', 'Name', 'Market']].head())