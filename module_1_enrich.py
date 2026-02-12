import pandas as pd
import requests
import os
import time
import re
from bs4 import BeautifulSoup

# --- Configuration ---
MASTER_FILE = "master_wics.csv"
DICT_FILE = "wics_dictionary.csv"


def load_data():
    """Loads the Stock Master Book and WICS Dictionary."""
    # 1. Load Master Book
    if os.path.exists(MASTER_FILE):
        df_master = pd.read_csv(MASTER_FILE, dtype={'Code': str})
    else:
        df_master = pd.DataFrame(columns=['Code', 'WICS_Code', 'Large', 'Medium', 'Small'])

    # 2. Load Dictionary
    if os.path.exists(DICT_FILE):
        df_dict = pd.read_csv(DICT_FILE, dtype={'WICS_Code': str})

        # Create a Lookup Map: Small Sector Name -> Row Data
        # We filter for rows that have a Small_Name (Level 3 or 4)
        # Note: Scraped names usually match the 'Small_Name' column (e.g. "반도체와반도체장비")
        # We strip spaces from keys to ensure robust matching

        wics_map = {}
        for _, row in df_dict.dropna(subset=['Small_Name']).iterrows():
            key = row['Small_Name'].replace(" ", "")
            wics_map[key] = {
                'WICS_Code': row['WICS_Code'],
                'Large': row['Large_Name'],
                'Medium': row['Medium_Name'],
                'Small': row['Small_Name']
            }

    else:
        print(f"⚠️ Warning: {DICT_FILE} not found. Run module_1_setup.py.")
        wics_map = {}

    return df_master, wics_map


def scrape_wics_sector(code):
    """
    Scrapes WiseReport for the WICS Small Sector Name.
    """
    url = f"https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx?cmp_cd={code}"
    headers = {
        'User-Agent': 'Mozilla/5.0'
    }

    try:
        r = requests.get(url, headers=headers, timeout=5)
        soup = BeautifulSoup(r.text, 'html.parser')

        # Find "WICS :"
        wics_element = soup.find(string=re.compile(r"WICS\s*:"))

        if wics_element:
            full_text = wics_element.strip()
            parts = full_text.split('WICS :')
            if len(parts) > 1:
                sector = parts[1].strip().replace('\xa0', '')
                return sector.replace(" ", "")  # Return cleaner name for lookup

        return None

    except Exception as e:
        print(f"   ❌ Error scraping {code}: {e}")
        return None


def update_master_book(missing_tickers, wics_map):
    """
    Scrapes -> Maps -> Saves.
    """
    print(f"🔄 Updating Master Book: Learning {len(missing_tickers)} new stocks...")

    new_rows = []

    for i, ticker in enumerate(missing_tickers):
        print(f"   [{i + 1}/{len(missing_tickers)}] Scraping {ticker}...", end="\r")

        # 1. Scrape
        sector_name = scrape_wics_sector(ticker)

        # 2. Map
        if sector_name and sector_name in wics_map:
            mapping = wics_map[sector_name]
            new_rows.append({
                'Code': ticker,
                'WICS_Code': mapping['WICS_Code'],
                'Large': mapping['Large'],
                'Medium': mapping['Medium'],
                'Small': mapping['Small']
            })
        else:
            new_rows.append({
                'Code': ticker,
                'WICS_Code': 'Unclassified',
                'Large': 'Unclassified',
                'Medium': 'Unclassified',
                'Small': f"Unmapped: {sector_name}" if sector_name else "Error"
            })

        time.sleep(0.1)

    print("\n✅ Update Complete.")

    # 3. Save
    df_new = pd.DataFrame(new_rows)
    df_old, _ = load_data()

    if not df_old.empty:
        df_combined = pd.concat([df_old, df_new], ignore_index=True)
        df_combined.drop_duplicates(subset=['Code'], keep='last', inplace=True)
    else:
        df_combined = df_new

    df_combined.to_csv(MASTER_FILE, index=False)
    return df_combined


def add_wics_info(df_snapshot):
    """
    Main Entry Point.
    """
    df_master, wics_map = load_data()

    snapshot_tickers = set(df_snapshot['Code'])
    known_tickers = set(df_master['Code']) if not df_master.empty else set()

    missing_tickers = list(snapshot_tickers - known_tickers)

    if missing_tickers:
        df_master = update_master_book(missing_tickers, wics_map)

    df_final = pd.merge(df_snapshot, df_master, on='Code', how='left')
    df_final.fillna("Unclassified", inplace=True)

    return df_final


# ... [End of module_1_enrich.py] ...

if __name__ == "__main__":
    import module_0

    print("🧪 Testing Module 1 Integration...")

    # 1. Fetch Raw Data (using fixed date for test)
    # Ensure module_0 is working and returns the Korean columns (Name, Code, Market + PyKRX columns)
    df_raw = module_0.fetch_krx_snapshot("20260210")
    print(f"   Raw Data Columns: {list(df_raw.columns)[:5]} ...")

    # 2. Run Enrich
    df_out = add_wics_info(df_raw)

    print("\n✅ Module 1 Output Columns (Should contain Market Data + WICS):")
    print(list(df_out.columns))

    # Check if 'Large' (Sector) and '시가총액' (Market Cap) are both present
    has_sector = 'Large' in df_out.columns
    has_cap = '시가총액' in df_out.columns or 'Marcap' in df_out.columns

    print(f"\n   Has Sector Info? {has_sector}")
    print(f"   Has Market Cap? {has_cap}")