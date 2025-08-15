import re
import datetime as dt
from pathlib import Path
import tempfile
import zipfile

import pandas as pd
import requests
import streamlit as st

# ============================================================
# Config
# ============================================================
st.set_page_config(page_title="FL 2025P NAL + TaxProper Merge", layout="wide")

BASE = "https://floridarevenue.com"
SITE = f"{BASE}/property/dataportal"
API = f"{SITE}/_api/web"

# SharePoint library + fixed 2025P folder (STRICT: we ONLY use this)
DOC_LIB = "/property/dataportal/Documents"
NAL_2025P = "/property/dataportal/Documents/PTO Data Portal/Tax Roll Data Files/NAL/2025P"

HEADERS = {
    "Accept": "application/json;odata=nometadata",
    "User-Agent": "Mozilla/5.0 (Streamlit/SharePoint)",
}

EXTRACT_DIR = Path("temp_extract")
EXTRACT_DIR.mkdir(exist_ok=True)

# ============================================================
# SharePoint helpers (no browser)
# ============================================================
def _enc(path: str) -> str:
    from urllib.parse import quote
    return quote(path, safe="/~ ")

def _sp_get_json(url: str):
    r = requests.get(url, headers=HEADERS, timeout=60)
    r.raise_for_status()
    return r.json()

def _sp_post_json(url: str, payload: dict):
    r = requests.post(
        url,
        json=payload,
        headers={
            **HEADERS,
            "Content-Type": "application/json;odata=nometadata",
            "Accept": "application/json;odata=nometadata",
        },
        timeout=60,
    )
    r.raise_for_status()
    return r.json()

def list_folders(server_relative: str):
    url = f"{API}/GetFolderByServerRelativeUrl('{_enc(server_relative)}')/Folders?$select=Name,ServerRelativeUrl"
    return _sp_get_json(url).get("value", [])

def list_files(server_relative: str):
    url = f"{API}/GetFolderByServerRelativeUrl('{_enc(server_relative)}')/Files?$select=Name,ServerRelativeUrl,Length"
    return _sp_get_json(url).get("value", [])

def render_list(folder_server_relative: str):
    list_url = (f"{API}/GetListUsingPath(DecodedUrl=@list)"
                f"/RenderListDataAsStream?@list=%27{_enc(DOC_LIB)}%27")
    payload = {
        "parameters": {
            "FolderServerRelativeUrl": folder_server_relative,
            "RenderOptions": 2,
            "ViewXml": "<View/>",
        }
    }
    data = _sp_post_json(list_url, payload)
    rows = data.get("Row", [])
    files, folders = [], []
    for row in rows:
        if str(row.get("FSObjType")) == "1":
            folders.append({
                "Name": row.get("FileLeafRef"),
                "ServerRelativeUrl": row.get("FileRef")
            })
        else:
            files.append({
                "Name": row.get("FileLeafRef"),
                "ServerRelativeUrl": row.get("FileRef"),
                "Length": row.get("FileSizeDisplay"),
            })
    return files, folders

def smart_list(folder: str):
    try:
        f1, d1 = list_files(folder), list_folders(folder)
        if f1 or d1:
            return f1, d1
    except Exception:
        pass
    try:
        return render_list(folder)
    except Exception:
        return [], []

def _norm(s: str) -> str:
    return re.sub(r"[^0-9A-Z]", "", (s or "").upper())

def list_all_2025p_files(max_depth: int = 3):
    files_out = []
    visited = set()
    queue = [(NAL_2025P, 0)]
    while queue:
        folder, depth = queue.pop(0)
        if folder in visited or depth > max_depth:
            continue
        visited.add(folder)
        files, folders = smart_list(folder)
        files_out.extend(files)
        for fol in folders:
            queue.append((fol["ServerRelativeUrl"], depth + 1))
    # de-dup by ServerRelativeUrl
    seen = set()
    unique_files = []
    for f in files_out:
        key = f.get("ServerRelativeUrl")
        if key and key not in seen:
            seen.add(key)
            unique_files.append(f)
    return unique_files

def find_2025p_zip(county: str):
    target = _norm(county)
    for f in list_all_2025p_files():
        nm = f.get("Name") or ""
        if nm.lower().endswith(".zip") and target in _norm(nm):
            return f
    return None

def download_zip(file_item: dict) -> Path:
    url = BASE + file_item["ServerRelativeUrl"]
    out = Path(tempfile.gettempdir()) / file_item["Name"]
    with requests.get(url, headers=HEADERS, stream=True, timeout=180) as r:
        r.raise_for_status()
        with open(out, "wb") as f:
            for chunk in r.iter_content(1024 * 64):
                if chunk:
                    f.write(chunk)
    return out

# ============================================================
# CSV helpers + merge
# ============================================================
def safe_read_csv(path: str | Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, dtype=str, low_memory=False)
    except UnicodeDecodeError:
        return pd.read_csv(path, dtype=str, low_memory=False, encoding="latin-1")

def extract_first_csv(zip_path: Path) -> Path | None:
    with zipfile.ZipFile(zip_path, "r") as zf:
        csvs = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csvs:
            return None
        csv_name = csvs[0]
        zf.extract(csv_name, path=EXTRACT_DIR)
        return EXTRACT_DIR / csv_name

def _find_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    lower = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in lower:
            return lower[cand.lower()]
    return None

def _clean_pid_series(s: pd.Series) -> pd.Series:
    return s.astype(str).map(lambda x: re.sub(r"[^0-9A-Za-z]", "", x or "").upper())

def _pick_target_width(roll_keys: pd.Series) -> int:
    # Heuristic: prefer the mode length of roll parcel IDs; fall back to max.
    lengths = roll_keys.str.len()
    if lengths.empty:
        return 0
    mode = lengths.mode()
    if len(mode):
        return int(mode.iloc[0])
    return int(lengths.max())

def do_merge(tp_file_path: str, county: str):
    # 1) Locate county 2025P ZIP
    file_item = find_2025p_zip(county)
    if not file_item:
        st.error(
            f"No **2025 Preliminary (2025P)** ZIP found for “{county}”. "
            "It might not be published in the 2025P folder yet, or the name differs."
        )
        return None, {}

    # 2) Download & extract
    zip_path = download_zip(file_item)
    csv_path = extract_first_csv(zip_path)
    if not csv_path:
        st.error("No CSV inside the downloaded ZIP.")
        return None, {}

    # 3) Load CSVs
    df_roll = safe_read_csv(csv_path)
    df_tp = safe_read_csv(tp_file_path)

    # 4) Detect parcel columns
    roll_col = _find_column(df_roll, ["PARCEL_ID", "PARCELID", "PARCEL", "PARCEL NUMBER", "PARCEL_NO", "PARCELNO"])
    if not roll_col:
        st.error(
            "Parcel column not found in the 2025P CSV (looked for PARCEL_ID / PARCELID / PARCEL / PARCEL NUMBER / PARCEL_NO)."
        )
        return None, {}

    tp_col = _find_column(df_tp, ["Parcel ID"])
    if not tp_col:
        st.error("Column 'Parcel ID' not found in your TaxProper CSV.")
        return None, {}

    # 5) Normalize IDs
    df_roll["_PID_"] = _clean_pid_series(df_roll[roll_col])
    df_tp["_PID_"] = _clean_pid_series(df_tp[tp_col])

    # 6) First-pass exact match
    first = pd.merge(df_roll, df_tp, on="_PID_", how="inner", suffixes=("_roll", "_tp"))

    # 7) Leading-zero retry ONLY for left-unmatched TP rows,
    #    padding to a target width inferred from roll IDs (mode length, else max).
    target_width = _pick_target_width(df_roll["_PID_"])
    left_unmatched_tp = df_tp[~df_tp["_PID_"].isin(first["_PID_"])].copy()

    retry = pd.DataFrame()
    if target_width > 0 and not left_unmatched_tp.empty:
        tp_retry = left_unmatched_tp.copy()
        tp_retry["_PID_"] = tp_retry["_PID_"].str.zfill(target_width)
        # only keep rows that actually changed after zfill to avoid duplicates
        changed = tp_retry["_PID_"] != left_unmatched_tp["_PID_"]
        tp_retry = tp_retry.loc[changed]
        if not tp_retry.empty:
            retry = pd.merge(df_roll, tp_retry, on="_PID_", how="inner", suffixes=("_roll", "_tp"))

    # 8) Combine & dedupe on the key
    combined = pd.concat([first, retry], ignore_index=True)
    if not combined.empty:
        combined = combined[~combined["_PID_"].duplicated(keep="first")]

    stats = {
        "roll_rows": len(df_roll),
        "tp_rows": len(df_tp),
        "first_pass_matches": len(first),
        "retry_matches": len(retry),
        "combined_matches": len(combined),
        "target_width": target_width,
        "zip_name": file_item.get("Name"),
    }

    return (combined if not combined.empty else None), stats

# ============================================================
# Minimal UI
# ============================================================
st.title("Florida 2025P NAL + TaxProper Merge")
st.caption("Scrapes the **official 2025P** folder, merges by Parcel ID, then retries unmatched by **left-padding with zeros** to the roll’s typical width.")

tp_file = st.file_uploader("Upload TaxProper CSV", type="csv")
county = st.text_input("County (e.g., Clay, Pinellas)")

run_clicked = st.button("Run Merge")

if run_clicked:
    if not tp_file or not county.strip():
        st.error("Please upload your TaxProper CSV and enter a county.")
    else:
        tp_path = "temp_tp.csv"
        with open(tp_path, "wb") as f:
            f.write(tp_file.getbuffer())

        with st.spinner("Fetching 2025P ZIP, extracting CSV, and merging…"):
            final_df, stats = do_merge(tp_path, county.strip())

        if final_df is not None:
            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("Roll rows", f"{stats['roll_rows']:,}")
            c2.metric("TaxProper rows", f"{stats['tp_rows']:,}")
            c3.metric("First-pass matches", f"{stats['first_pass_matches']:,}")
            c4.metric("Retry matches", f"{stats['retry_matches']:,}")
            c5.metric("Total matches", f"{stats['combined_matches']:,}")
            st.caption(f"2025P file: **{stats['zip_name']}** • Zero-fill width used: **{stats['target_width']}**")

            st.dataframe(final_df.head(50), use_container_width=True)
            csv_bytes = final_df.to_csv(index=False).encode("utf-8")
            st.download_button("Download Results CSV", csv_bytes, "merged_results_2025P.csv")
        else:
            st.warning("No matches found (or 2025P county file not available).")
