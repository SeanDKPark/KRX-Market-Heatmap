import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
import pytz

# --- Import Custom Modules ---
import module_0  # Date Validator & KRX Fetcher
import module_1_enrich  # WICS Master Book Manager
import module_2  # Data formatter

# --- Configuration ---
PAGE_TITLE = "KRX Market Heatmap (WICS)"
st.set_page_config(layout="wide", page_title=PAGE_TITLE)

# --- CUSTOM CSS ---
st.markdown("""
    <style>
        /* 1. Aggressive Padding Removal (Main & Sidebar) */
        .block-container {
            padding-top: 1rem !important;
            padding-bottom: 0rem !important;
            margin-top: 0rem !important;
        }

        /* Sidebar specific squeeze */
        section[data-testid="stSidebar"] .block-container {
            padding-top: 0rem !important; /* No top space */
            padding-bottom: 1rem !important;
        }

        /* 2. Tight Separator Lines */
        hr {
            margin-top: 0.5rem !important;
            margin-bottom: 0.5rem !important;
            border-color: #e6e6e6;
        }

        /* 3. Compact Title */
        h1 {
            font-size: 1.8rem !important;
            margin-bottom: 0rem !important;
            padding-bottom: 0rem !important;
        }

        /* 4. Widget Label Spacing */
        .stMarkdown p {
            margin-bottom: 0px !important;
        }
        div[data-testid="stVerticalBlock"] > div {
            gap: 0.5rem !important; /* Reduce gap between widgets */
        }

        /* 5. Metrics Bar Style */
        .metric-container {
            display: flex; justify_content: center; align_items: center;
            padding: 8px; background-color: #f0f2f6; border-radius: 5px;
            margin-bottom: 10px; font-size: 14px; font-weight: 500;
        }
        .metric-item { margin: 0 15px; }
        .metric-value { font-weight: bold; color: #31333F; }
    </style>
""", unsafe_allow_html=True)


# --- HELPER: Toggle Button Group (Visual Persistence) ---
def render_toggle_group(label, options, key_prefix, default_all=True, columns=2):
    """
    Renders a grid of buttons that act like checkboxes.
    Returns: List of selected options.
    """
    if label:
        st.write(f"**{label}**")

    # Initialize session state for this group
    if f"{key_prefix}_selected" not in st.session_state:
        st.session_state[f"{key_prefix}_selected"] = options if default_all else []

    selected_items = st.session_state[f"{key_prefix}_selected"]

    # Create Layout (e.g., 2 columns for buttons)
    cols = st.columns(columns)

    for i, option in enumerate(options):
        col_idx = i % columns
        is_active = option in selected_items

        # Visual State: Primary = On, Secondary = Off (Gray)
        btn_type = "primary" if is_active else "secondary"

        if cols[col_idx].button(option, key=f"{key_prefix}_btn_{i}", type=btn_type, use_container_width=True):
            # Toggle Logic
            if is_active:
                selected_items.remove(option)
            else:
                selected_items.append(option)
            st.rerun()  # Refresh to update button color immediately

    return selected_items


def main():
    # --- Sidebar ---
    with st.sidebar:
        st.sidebar.title("📅 Settings")
    
        # STATIC MODE: Disable the date picker
        st.sidebar.info("🔒 Date fixed in Demo Mode")
        selected_date = st.sidebar.date_input(
            "Select Date",
            value=datetime(2026, 2, 12), # Set this to your snapshot date
            disabled=True  # Gray it out
        )

        # 2. Load Button
        if st.button("🚀 Load Market Data", type="primary", use_container_width=True):
            st.session_state['run_analysis'] = True
            st.session_state['target_date'] = selected_date

        st.markdown("---")

        # 3. View & Size
        st.write("**Global Settings**")
        view_mode = st.radio("Hierarchy Depth", ["Simple (Small Sector)", "Full (Large -> Medium -> Small)"])
        size_mode_label = st.selectbox("Box Size Standard",
                                       ["Market Cap (Importance)", "Volatility (Action)", "Trading Value (Liquidity)"])

        st.markdown("---")

        # 4. Market Filter (Custom Toggles)
        # Using 2 columns for KOSPI / KOSDAQ
        selected_markets = render_toggle_group(
            "Market",
            ['KOSPI', 'KOSDAQ', 'KONEX'],
            "market_toggle",
            default_all=True,  # Default both selected
            columns=3
        )

        st.markdown("---")

        # 5. Size Filter (Custom Toggles)
        # Using 2x2 grid for Tiers
        tier_labels = [
            "Large (>10T)",
            "Mid (1T~10T)",
            "Small (100B~1T)",
            "Micro (<100B)"
        ]
        # Map labels back to logic keys if needed, but we can filter by index or string
        selected_tiers = render_toggle_group(
            "Size Filter (Tiers)",
            tier_labels,
            "tier_toggle",
            default_all=True,
            columns=2
        )

        # Range Toggle
        use_custom_range = st.checkbox("Custom Range Filter", value=False)
        if use_custom_range:
            c1, c2 = st.columns(2)
            min_cap = c1.number_input("Min (억)", value=0, step=100)
            max_cap = c2.number_input("Max (억)", value=5000000, step=1000)

    # --- Main Logic ---
    st.title(f"📊 {PAGE_TITLE}")

    if st.session_state.get('run_analysis'):
        target_date = st.session_state['target_date']

        with st.spinner(f"Fetching data for {target_date}..."):
            try:
                # 1. Pipeline Execution
                df_raw = module_0.fetch_krx_snapshot(target_date)
                if df_raw.empty:
                    st.error("No data found.")
                    return

                df_wics = module_1_enrich.add_wics_info(df_raw)
                df = module_2.enrich_data(df_raw, df_wics)

                # --- PREPROCESSING ---
                df['Marcap_100M'] = df['Marcap'] / 100_000_000

                # 2. Apply Market Filter
                # If nothing selected, show nothing (or all? usually nothing implies reset, but let's stick to strict filter)
                if not selected_markets:
                    st.warning("Please select at least one Market.")
                    return
                df = df[df['Market'].isin(selected_markets)]

                # 3. Apply Tier Filter
                # Map the readable labels back to logic
                mask_tiers = pd.Series(False, index=df.index)
                if not selected_tiers:
                    st.warning("Please select at least one Size Tier.")  # Or handle as "Show None"
                else:
                    if "Large" in str(selected_tiers):
                        mask_tiers |= (df['Marcap_100M'] >= 100000)
                    if "Mid" in str(selected_tiers):
                        mask_tiers |= (df['Marcap_100M'] >= 10000) & (df['Marcap_100M'] < 100000)
                    if "Small" in str(selected_tiers):
                        mask_tiers |= (df['Marcap_100M'] >= 1000) & (df['Marcap_100M'] < 10000)
                    if "Micro" in str(selected_tiers):
                        mask_tiers |= (df['Marcap_100M'] < 1000)

                # 4. Apply Range Filter
                if use_custom_range:
                    mask_range = (df['Marcap_100M'] >= min_cap) & (df['Marcap_100M'] <= max_cap)
                else:
                    mask_range = True

                df_filtered = df[mask_tiers & mask_range].copy()

                # --- VISUALIZATION ---
                display_dashboard(df_filtered, view_mode, size_mode_label)

            except Exception as e:
                st.error(f"An error occurred: {e}")
                st.exception(e)


def display_dashboard(df, view_mode, size_mode_label):
    if df.empty:
        st.warning("No stocks match your filter criteria.")
        return

    # --- 1. Metrics Bar ---
    total_cap = df['Marcap'].sum() / 100_000_000
    avg_return = df['ChagesRatio'].mean()
    vol = df['Amount'].sum() / 100_000_000
    count = len(df)

    st.markdown(f"""
        <div class="metric-container">
            <span class="metric-item">Stocks: <span class="metric-value">{count}</span></span>
            <span class="metric-item">Total Cap: <span class="metric-value">{total_cap:,.0f} 억</span></span>
            <span class="metric-item">Volume: <span class="metric-value">{vol:,.0f} 억</span></span>
            <span class="metric-item">Avg Return: <span class="metric-value">{avg_return:+.2f}%</span></span>
        </div>
    """, unsafe_allow_html=True)

    # --- 2. Split Layout ---
    col_map, col_list = st.columns([3.5, 1.5])

    # --- LEFT: Heatmap ---
    with col_map:
        st.subheader(f"Market Map ({size_mode_label})")

        path = ['Small', 'Label'] if "Simple" in view_mode else ['Large', 'Medium', 'Small', 'Label']

        if "Market Cap" in size_mode_label:
            size_col = 'Marcap'
        elif "Volatility" in size_mode_label:
            df['Abs_Change'] = df['ChagesRatio'].abs()
            size_col = 'Abs_Change'
        elif "Trading Value" in size_mode_label:
            size_col = 'Amount'
        else:
            size_col = 'Marcap'

        fig = px.treemap(
            df,
            path=[px.Constant("KRX")] + path,
            values=size_col,
            color='Color_Value',
            color_continuous_scale=['blue', 'white', 'red'],
            color_continuous_midpoint=0,
            range_color=[-30, 30],
            hover_data={
                'Name': True, 'Close': True, 'ChagesRatio': ':.2f',
                'Marcap_Disp': True, 'Label': False, 'Color_Value': False
            },
            height=800
        )
        fig.update_traces(
            textinfo="label+value",
            texttemplate="%{label}",
            marker=dict(line=dict(width=0.5, color='grey')),
            root_color="lightgrey"
        )

        # Increased Top Margin to fix Breadcrumb overlapping Title
        fig.update_layout(margin=dict(t=40, l=10, r=10, b=10))
        st.plotly_chart(fig, use_container_width=True)

    # --- RIGHT: Ranking List ---
    with col_list:
        c_sort, c_num = st.columns([2, 1])
        with c_sort:
            rank_metric = st.selectbox(
                "Sort List By",
                ["Market Cap", "Price Change (Gainers)", "Price Change (Losers)", "Trading Value (Amt)",
                 "Trading Volume (Shares)"]
            )
        with c_num:
            top_n = st.number_input("Top N", min_value=5, max_value=100, value=15, step=5)

        st.divider()

        # Sort Logic
        sort_col = "Marcap"
        ascending = False

        if rank_metric == "Market Cap":
            sort_col = "Marcap"
        elif "Gainers" in rank_metric:
            sort_col = "ChagesRatio"; ascending = False
        elif "Losers" in rank_metric:
            sort_col = "ChagesRatio"; ascending = True
        elif "Trading Value" in rank_metric:
            sort_col = "Amount"
        elif "Trading Volume" in rank_metric:
            sort_col = "Volume"

        df_top = df.sort_values(by=sort_col, ascending=ascending).head(top_n).copy()

        # Display Format
        df_disp = pd.DataFrame()
        df_disp['Name'] = df_top['Name']

        if 'Marcap' in df_top.columns: df_disp['Cap (억)'] = (df_top['Marcap'] / 100_000_000).apply(
            lambda x: f"{x:,.0f}")
        if 'ChagesRatio' in df_top.columns: df_disp['Chg (%)'] = df_top['ChagesRatio'].apply(lambda x: f"{x:+.2f}%")
        if 'Amount' in df_top.columns: df_disp['Amt (억)'] = (df_top['Amount'] / 100_000_000).apply(
            lambda x: f"{x:,.0f}")
        if 'Volume' in df_top.columns: df_disp['Vol (주)'] = df_top['Volume'].apply(lambda x: f"{x:,.0f}")

        # Column Order
        sort_map = {'Marcap': 'Cap (억)', 'ChagesRatio': 'Chg (%)', 'Amount': 'Amt (억)', 'Volume': 'Vol (주)'}
        primary = sort_map.get(sort_col, 'Cap (억)')
        cols = ['Name', primary] + [c for c in df_disp.columns if c not in ['Name', primary]]

        st.dataframe(df_disp[cols], hide_index=True, use_container_width=True, height=700)


if __name__ == "__main__":

    main()
