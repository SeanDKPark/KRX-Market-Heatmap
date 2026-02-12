import pandas as pd
import numpy as np


def enrich_data(df_snapshot, df_enhanced):
    """
    Formats the data for visualization.
    df_enhanced comes from Module 1 and likely has _x/_y suffixes.
    """
    print("🎨 Module 2: Formatting data for visualization...")

    # 1. Use the enhanced dataframe directly (Ignore df_snapshot to prevent double merge)
    df_final = df_enhanced.copy()

    # 2. Helper: Consolidate Duplicate Columns (Fix _x / _y issues)
    # This function looks for a list of possible column names and maps the first found to the 'Target' name.
    def consolidate_column(df, target, candidates):
        # If target already exists and is good, do nothing
        if target in df.columns:
            return

        # Check candidates
        for candidate in candidates:
            if candidate in df.columns:
                df[target] = df[candidate]
                return

        # Fallback if nothing found
        print(f"   ⚠️ Warning: Could not find data for '{target}'. Setting to 0.")
        df[target] = 0

    # 3. Apply Consolidation
    # We map the Korean names (and their _x/_y variants) to English standard names

    # Market Cap (User log showed 시가총액_x and 시가총액_y)
    consolidate_column(df_final, 'Marcap', ['시가총액', '시가총액_x', '시가총액_y', 'Marcap'])

    # Close Price
    consolidate_column(df_final, 'Close', ['종가', '종가_x', '종가_y', 'Close'])

    # Change Ratio (User log showed 등락률)
    consolidate_column(df_final, 'ChagesRatio', ['등락률', '등락률_x', 'Change', 'ChagesRatio'])

    # Volume
    consolidate_column(df_final, 'Volume', ['거래량', '거래량_x', '거래량_y', 'Volume'])

    # Trading Value (Amount)
    consolidate_column(df_final, 'Amount', ['거래대금', '거래대금_x', '거래대금_y', 'Amount'])

    # 4. Fill Missing Sector Info
    fill_cols = ['Large', 'Medium', 'Small']
    for col in fill_cols:
        if col in df_final.columns:
            df_final[col] = df_final[col].fillna("Unclassified")
        else:
            df_final[col] = "Unclassified"

    # 5. Create 'Label' for Treemap
    # Defensive: Use .get() to prevent crashes
    def format_label(row):
        name = str(row.get('Name', row.get('Code', 'Unknown')))
        try:
            change_val = float(row.get('ChagesRatio', 0.0))
            return f"{name}\n{change_val:+.2f}%"
        except:
            return name

    df_final['Label'] = df_final.apply(format_label, axis=1)

    # 6. Create 'Color_Value' for Heatmap
    # Convert to numeric first to handle any bad data
    df_final['ChagesRatio'] = pd.to_numeric(df_final['ChagesRatio'], errors='coerce').fillna(0)
    df_final['Color_Value'] = df_final['ChagesRatio'].clip(-30, 30)

    # 7. Format Market Cap Display
    df_final['Marcap'] = pd.to_numeric(df_final['Marcap'], errors='coerce').fillna(0)
    df_final['Marcap_Disp'] = df_final['Marcap'].apply(lambda x: f"{x / 100_000_000:,.0f} 억")

    # 8. Final Column Selection
    # Keep only clean English columns
    desired_cols = [
        'Code', 'Name', 'Market', 'Close', 'ChagesRatio', 'Volume', 'Amount', 'Marcap',
        'Large', 'Medium', 'Small',
        'Label', 'Color_Value', 'Marcap_Disp', 'Snapshot_Date'
    ]

    existing_cols = [c for c in desired_cols if c in df_final.columns]
    df_final = df_final[existing_cols]

    print(f"✅ Data Ready. Rows: {len(df_final)}")
    return df_final


if __name__ == "__main__":
    print("Testing Module 2...")