import pandas as pd
import re
from pypdf import PdfReader

# --- Configuration ---
PDF_PATH = "WICS Methodology.pdf"
DICT_OUTPUT = "wics_dictionary.csv"  # This will match the structure of krx_wics_reference.csv


def generate_wics_dictionary():
    print(f"📖 Reading PDF: {PDF_PATH}...")

    try:
        reader = PdfReader(PDF_PATH)
        full_text = ""
        for page in reader.pages:
            full_text += page.extract_text() + "\n"
    except FileNotFoundError:
        print(f"❌ Error: {PDF_PATH} not found. Please upload it.")
        return

    # 1. Extract Code -> Name pairs
    # Pattern looks for: Code (2-8 digits) followed by Name
    # Example: "10 에너지", "101010 에너지장비"
    code_to_name = {}

    # We split by lines to handle the text flow
    lines = full_text.split('\n')

    for line in lines:
        # Regex: boundary, digits(2-8), whitespace, text(not digits)
        matches = re.finditer(r'\b(\d{2,8})\s+([^\d\n]+)', line)
        for m in matches:
            code = m.group(1)
            raw_name = m.group(2).strip()

            # WICS codes are even length (2, 4, 6, 8)
            if len(code) % 2 == 0:
                clean_name = raw_name.replace(" ", "")  # Remove spaces for cleaner matching
                # Store the longest code found (in case of duplicates/fragments)
                if code not in code_to_name:
                    code_to_name[code] = raw_name.strip()  # Keep original name with spaces for display

    # 2. Build the Hierarchical Table
    # Structure: WICS_Code, WICS_Name, Large, Medium, Small, Micro
    dict_rows = []

    sorted_codes = sorted(code_to_name.keys())

    for code in sorted_codes:
        name = code_to_name[code]

        # Initialize Hierarchy
        large = None
        medium = None
        small = None
        micro = None

        # Determine Hierarchy based on Code Length
        # Large (2)
        if len(code) >= 2:
            large = code_to_name.get(code[:2], None)

        # Medium (4)
        if len(code) >= 4:
            medium = code_to_name.get(code[:4], None)

        # Small (6)
        if len(code) >= 6:
            small = code_to_name.get(code[:6], None)

        # Micro (8)
        if len(code) >= 8:
            micro = code_to_name.get(code[:8], None)

        dict_rows.append({
            "WICS_Code": code,
            "WICS_Name": name,
            "Large_Name": large,
            "Medium_Name": medium,
            "Small_Name": small,
            "Micro_Name": micro
        })

    # 3. Save
    df_dict = pd.DataFrame(dict_rows)

    # Reorder columns to match reference exactly
    cols = ['WICS_Code', 'WICS_Name', 'Large_Name', 'Medium_Name', 'Small_Name', 'Micro_Name']
    df_dict = df_dict[cols]

    df_dict.to_csv(DICT_OUTPUT, index=False, encoding='utf-8-sig')
    print(f"✅ Dictionary Created: {DICT_OUTPUT} ({len(df_dict)} rows)")
    print(df_dict.head())


if __name__ == "__main__":
    generate_wics_dictionary()