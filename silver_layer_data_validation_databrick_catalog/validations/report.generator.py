import os
from datetime import datetime

OUTPUT_DIR = r"C:\Users\MODICHERLA\OneDrive - Hexalytics, Inc\Documents\PyCharmMiscProject\silver_layer_data_validation\output"

def generate_excel_report(df, table_name, status):

    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    file_name = f"{table_name}_{timestamp}_{status}.xlsx"
    full_path = os.path.join(OUTPUT_DIR, file_name)

    df.to_excel(full_path, index=False)

    print(f"✅ Report Generated: {full_path}")