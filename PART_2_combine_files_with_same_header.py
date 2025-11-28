import os
import pandas as pd

data = "data/PART_1_xls_to_csv/"
out_dir = "data/PART_2_xls_to_csv_combined_header/"


def year_from_path(path, base_dir):
    rel = os.path.relpath(path, start=base_dir)
    parts = rel.split(os.sep)
    for part in parts:
        if len(part) == 4 and part.isdigit():
            return int(part)
    for part in parts:
        if len(part) >= 4 and part[:4].isdigit():
            return int(part[:4])
    return None


def collect_csv_files(base_dir, ext=".csv", **kwargs):
    file_infos = []
    for root, _, files in os.walk(base_dir):
        for file in files:
            if not file.lower().endswith(ext.lower()):
                continue
            path = os.path.join(root, file)
            try:
                df = pd.read_csv(path, **kwargs)
                df.columns = [col.strip().lower() for col in df.columns]
                header = tuple(df.columns)
                year = year_from_path(path, base_dir)
                file_infos.append({"path": path, "df": df, "header": header, "year": year})
            except Exception as e:
                print(f"Error loading {path}: {e}")
    return file_infos


def check_header_differences(file_infos):
    if not file_infos:
        print("No CSV files found.")
        return
    header_map = {}
    for info in file_infos:
        header_map.setdefault(info["header"], []).append(info["path"])
    headers = list(header_map.keys())
    base = headers[0]
    if len(headers) == 1:
        print("All CSV files have the same (lowercased) header format")
        return
    print("Differences compared to the first header:\n")
    print("Baseline header:", base, "\n")
    base_set = set(base)
    for i, hdr in enumerate(headers[1:], start=2):
        missing = sorted(base_set - set(hdr))
        extra = sorted(set(hdr) - base_set)
        print(f"Header format #{i}: {hdr}")
        if missing:
            print(f"  Missing columns: {missing}")
        if extra:
            print(f"  Extra columns:   {extra}")
        print("Files:")
        for p in header_map[hdr]:
            print(f" - {p}")
        print()


def combine_same_format_files(file_infos, out_dir, base_dir, add_source=True):
    os.makedirs(out_dir, exist_ok=True)
    groups = {}
    for info in file_infos:
        df = info["df"]
        header = list(info["header"])
        cols_with_year = list(header)
        if "year" not in cols_with_year:
            cols_with_year.append("year")
        key = frozenset(cols_with_year)
        if key not in groups:
            order = list(header)
            if "year" not in order:
                order.append("year")
            groups[key] = {"order": order, "files": []}
        groups[key]["files"].append(info)
    if not groups:
        print("No CSV files found.")
        return {}
    outputs = {}
    for idx, (key, info) in enumerate(groups.items(), start=1):
        order = info["order"]
        out_path = os.path.join(out_dir, f"group_{idx}.csv")
        frames = []
        for file_info in info["files"]:
            df = file_info["df"].copy()
            if "year" not in df.columns:
                yr = file_info["year"]
                df["year"] = yr if yr is not None else pd.NA
            if add_source:
                df["__source_file"] = os.path.relpath(file_info["path"], start=base_dir)
            cols = order + (["__source_file"] if add_source else [])
            df = df.reindex(columns=cols)
            frames.append(df)
        if not frames:
            print(f"(skip) No readable files for group #{idx}")
            continue
        combined = pd.concat(frames, ignore_index=True)
        combined.to_csv(out_path, index=False)
        outputs[out_path] = [fi["path"] for fi in info["files"]]
        print(f"Wrote {len(combined)} rows from {len(info['files'])} files -> {out_path}")
    return outputs


if __name__ == "__main__":
    file_infos = collect_csv_files(data)
    check_header_differences(file_infos)
    combine_same_format_files(file_infos, out_dir, base_dir=data, add_source=False)
