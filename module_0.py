import pandas as pd
import streamlit as st
import os

# This function is no longer needed but kept to prevent import errors in app.py if it calls it
def get_latest_business_day(target_date=None):
    return "20260212" # Return your snapshot date

@st.cache_data
def fetch_krx_snapshot(target_date):
    """
    STATIC MODE: Loads pre-fetched data from CSV.
    Ignores 'target_date' because we only have one snapshot.
    """
    file_path = "krx_static_data.csv"
    
    if not os.path.exists(file_path):
        st.error(f"❌ Static data file '{file_path}' not found! Did you upload it to GitHub?")
        return pd.DataFrame()

    # Load Data
    # IMPORTANT: Ensure 'Code' is read as string to preserve leading zeros (e.g., "005930")
    try:
        df = pd.read_csv(file_path, dtype={'Code': str})
        
        # Show a friendly "Demo Mode" warning
        st.warning(f"📢 **DEMO MODE:** Showing cached data from **{df['Snapshot_Date'].iloc[0]}**. Live fetching is disabled in this web demo.")
        
        return df
    except Exception as e:
        st.error(f"Error loading static file: {e}")
        return pd.DataFrame()
