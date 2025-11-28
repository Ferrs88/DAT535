import os
import pandas as pd

data_directory = "data/PART_2_xls_to_csv_combined_header/"
combined_output_path = "data/PART_3_csv/all_combined_data.csv"

def _coalesce_into(df, target, src, changes):
    if src not in df.columns:
        return
    if target in df.columns:
        df[target] = df[target].where(df[target].notna(), df[src])
        df.drop(columns=[src], inplace=True)
        changes.append(f"{src} → {target} (coalesced)")
    else:
        df.rename(columns={src: target}, inplace=True)
        changes.append(f"{src} → {target} (renamed)")


def unify_schema_in_df(df):
    changes = []
    df = df.copy()

    _coalesce_into(df, "occ_title", "occ_titl", changes)
    _coalesce_into(df, "group", "o_group", changes)
    _coalesce_into(df, "group", "occ_group", changes)

    cols = list(df.columns)
    for c in cols:
        if c.startswith("a_wpct"):
            target = "a_pct" + c[len("a_wpct"):]
            _coalesce_into(df, target, c, changes)
    cols = list(df.columns)
    for c in cols:
        if c.startswith("h_wpct"):
            target = "h_pct" + c[len("h_wpct"):]
            _coalesce_into(df, target, c, changes)

    return df, changes


def collect_csv_files(base_dir, **read_csv_kwargs):
    file_infos = []
    for root, _, files in os.walk(base_dir):
        for file in files:
            if not file.lower().endswith(".csv"):
                continue
            path = os.path.join(root, file)
            try:
                df = pd.read_csv(path, **read_csv_kwargs)
                df.columns = [col.strip().lower() for col in df.columns]
                header = tuple(df.columns)
                file_infos.append({"path": path, "df": df, "header": header})
            except Exception as e:
                print(f"Error loading {path}: {e}")
    return file_infos


def build_header_map(file_infos):
    header_map = {}
    for info in file_infos:
        header_map.setdefault(info["header"], []).append(info["path"])
    return header_map


def check_header_differences(header_map):
    if not header_map:
        print("No CSV files found.")
        return

    headers = list(header_map.keys())
    base = headers[0]
    if len(headers) == 1:
        print("--> All CSV files have the same (lowercased) header format.")
        return

    print("--> Differences compared to the first header:\n")
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


def expand_to_include_extra_columns(file_infos):
    if not file_infos:
        return

    headers = [info["header"] for info in file_infos]
    base = list(headers[0])
    base_set = set(base)

    any_missing_from_base = False
    extra_cols_in_order = []
    extra_seen = set()

    for hdr in headers[1:]:
        hdr_list = list(hdr)
        hdr_set = set(hdr_list)

        missing = base_set - hdr_set
        if missing:
            any_missing_from_base = True

        for c in hdr_list:
            if c not in base_set and c not in extra_seen:
                extra_cols_in_order.append(c)
                extra_seen.add(c)

    if any_missing_from_base or not extra_cols_in_order:
        return

    full_header = base + extra_cols_in_order

    for info in file_infos:
        df = info["df"].copy()
        for col in full_header:
            if col not in df.columns:
                df[col] = pd.NA
        df = df[full_header]
        info["df"] = df
        info["header"] = tuple(full_header)


def unify_dirs(dirs, **read_csv_kwargs):
    if isinstance(dirs, str):
        dirs = [dirs]

    all_infos = []

    for base_dir in dirs:
        print(f"\n=== Directory: {base_dir} ===")
        file_infos = collect_csv_files(base_dir, **read_csv_kwargs)

        print("Before:")
        pre_map = build_header_map(file_infos)
        check_header_differences(pre_map)

        for info in file_infos:
            path = info["path"]
            df = info["df"]
            try:
                new_df, changes = unify_schema_in_df(df)
                if not changes:
                    continue
                print(f"→ {os.path.relpath(path)}")
                for c in changes:
                    print(f"   - {c}")
                info["df"] = new_df
                info["header"] = tuple(new_df.columns)
            except Exception as e:
                print(f"Error processing {path}: {e}")

        expand_to_include_extra_columns(file_infos)

        print("\nAfter:")
        post_map = build_header_map(file_infos)
        check_header_differences(post_map)

        all_infos.extend(file_infos)
    return all_infos


def combine_unified_files(file_infos, output_path):
    if not file_infos:
        print("No files to combine.")
        return

    frames = [info["df"] for info in file_infos]
    combined = pd.concat(frames, ignore_index=True)

    for col in combined.columns:
        if col == "occ_title":
            continue
        s = combined[col]
        mask = s.notna()
        combined[col] = s.where(~mask, s[mask].astype(str).str.replace(r"\s+", "", regex=True))

    out_dir = os.path.dirname(output_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    combined.to_csv(output_path, index=False)
    print(f"Combined {len(file_infos)} files, {len(combined)} rows -> {output_path}")


if __name__ == "__main__":
    infos = unify_dirs(data_directory)
    combine_unified_files(infos, combined_output_path)
