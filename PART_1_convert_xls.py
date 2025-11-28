import os
import pandas as pd

input_directory = "data/PART_0_xls/"
output_directory = "data/PART_1_xls_to_csv/"
scan_depth = 300  # how many rows to scan to find the header

os.makedirs(output_directory, exist_ok=True)

for root, _, files in os.walk(input_directory):
    for file in files:
        if not (file.lower().endswith(".xls") or file.lower().endswith(".xlsx")):
            continue
        if file.lower() in ["field_descriptions.xls", "field_descriptions.xlsx"]:
            continue

        file_path = os.path.join(root, file)
        save_dir = os.path.join(output_directory, os.path.relpath(root, input_directory))
        os.makedirs(save_dir, exist_ok=True)

        engine = "openpyxl" if file.lower().endswith(".xlsx") else "xlrd"
        xls = pd.ExcelFile(file_path, engine=engine)

        for sheet in xls.sheet_names:
            if sheet.lower() in ["field descriptions", "updatetime"]:
                continue

            df_raw = pd.read_excel(file_path, sheet_name=sheet, header=None, engine=engine)

            # skip completely empty sheets so iloc[0] doesn't crash
            if df_raw.shape[0] == 0:
                continue

            header_row = 0
            for i in range(min(scan_depth, df_raw.shape[0])):
                row_values = [str(x).strip().lower() for x in df_raw.iloc[i]]
                if "occ_code" in row_values:
                    header_row = i
                    break

            header = df_raw.iloc[header_row].astype(str).str.strip()
            df = df_raw.iloc[header_row + 1:].copy()
            df.columns = [str(x).strip() for x in header]

            suffix = f"_{sheet}" if len(xls.sheet_names) > 1 else ""
            out_path = os.path.join(save_dir, f"{os.path.splitext(file)[0]}{suffix}.csv")

            df.to_csv(out_path, index=False, encoding="utf-8-sig")

print("Conversion completed.")