import re
from pathlib import Path
import tempfile
import zipfile
from typing import Union, List, Optional

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

DOC_LIB = "/property/dataportal/Documents"
NAL_2025P = "/property/dataportal/Documents/PTO Data Portal/Tax Roll Data Files/NAL/2025P"

HEADERS = {
    "Accept": "application/json;odata=nometadata",
    "User-Agent": "Mozilla/5.0 (Streamlit/SharePoint)",
}

EXTRACT_DIR = Path("temp_extract")
EXTRACT_DIR.mkdir(exist_ok=True)


# ============================================================
# SharePoint helpers
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
        if str(row.get("FSObjType")) == "1":  # folder
            folders.append({
                "Name": row.get("FileLeafRef"),
                "ServerRelativeUrl": row.get("FileRef")
            })
        else:  # file
            files.append({
                "Name": row.get("FileLeafRef"),
                "ServerRelativeUrl": row.get("FileRef"),
                # Render API gives a display string; REST gives bytes in "Length"
                "Length": row.get("FileSizeDisplay") or row.get("Length"),
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
    seen = set()
    unique_files = []
    for f in files_out:
        key = f.get("ServerRelativeUrl")
        if key and key not in seen:
            seen.add(key)
            unique_files.append(f)
    return unique_files

def find_2025p_zip(county: str):
    # kept for compatibility; no longer used in UI
    target = _norm(county)
    for f in list_all_2025p_files():
        nm = f.get("Name") or ""
        if nm.lower().endswith(".zip") and target in _norm(nm):
            return f
    return None

def download_zip(file_item: dict) -> Path:
    url = BASE + file_item["ServerRelativeUrl"]
    out = Path(tempfile.gettempdir()) / file_item["Name"]
    with requests.get(url, headers=HEADERS, stream=True, timeout=None) as r:
        r.raise_for_status()
        with open(out, "wb") as f:
            for chunk in r.iter_content(1024 * 64):
                if chunk:
                    f.write(chunk)
    return out


# ============================================================
# CSV helpers + merge
# ============================================================
def safe_read_csv(path: Union[str, Path]) -> pd.DataFrame:
    try:
        return pd.read_csv(path, dtype=str, low_memory=False)
    except UnicodeDecodeError:
        return pd.read_csv(path, dtype=str, low_memory=False, encoding="latin-1")

def extract_first_csv(zip_path: Path) -> Optional[Path]:
    with zipfile.ZipFile(zip_path, "r") as zf:
        csvs = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csvs:
            return None
        csv_name = csvs[0]
        zf.extract(csv_name, path=EXTRACT_DIR)
        return EXTRACT_DIR / csv_name

def _find_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    lower = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in lower:
            return lower[cand.lower()]
    return None

def _clean_pid_series(s: pd.Series) -> pd.Series:
    return s.astype(str).map(lambda x: re.sub(r"[^0-9A-Za-z]", "", x or "").upper())

def _pick_target_width(roll_keys: pd.Series) -> int:
    lengths = roll_keys.str.len()
    if lengths.empty:
        return 0
    mode = lengths.mode()
    if len(mode):
        return int(mode.iloc[0])
    return int(lengths.max())

def do_merge_selected(tp_file_path: Union[str, Path], file_item: dict):
    """Chunked merge for very large county files using a selected SharePoint file.
    Includes ALL TaxProper columns in the output (with '_TP' suffix on name collisions)."""
    import csv

    if not file_item:
        st.error("No 2025P ZIP file selected.")
        return None, {}

    # --- Load roll file (from ZIP) ---
    zip_path = download_zip(file_item)
    csv_path = extract_first_csv(zip_path)
    if not csv_path:
        st.error("No CSV inside the downloaded ZIP.")
        return None, {}

    # --- Load TaxProper CSV ---
    df_tp = safe_read_csv(tp_file_path)
    tp_col = _find_column(df_tp, ["Parcel ID"])
    if not tp_col:
        st.error("Column 'Parcel ID' not found in your TaxProper CSV.")
        return None, {}

    # Normalize TP parcel IDs
    df_tp["_PID_"] = _clean_pid_series(df_tp[tp_col])

    # We'll create a zero-filled key later once we infer the roll's target width
    tp_has_zf = False

    # --- Output CSV path ---
    matches_path = Path(tempfile.gettempdir()) / "merged_results_2025P.csv"
    if matches_path.exists():
        matches_path.unlink()

    # --- Stats ---
    first_pass_count = 0
    retry_count = 0
    roll_rows_total = 0
    target_width = None
    roll_col = None

    chunksize = 50_000
    with pd.read_csv(csv_path, dtype=str, low_memory=False, chunksize=chunksize) as reader:
        for chunk in reader:
            roll_rows_total += len(chunk)

            # Identify the roll parcel column & typical width once
            if roll_col is None:
                roll_col = _find_column(
                    chunk,
                    ["PARCEL_ID", "PARCELID", "PARCEL", "PARCEL NUMBER", "PARCEL_NO", "PARCELNO"]
                )
                if not roll_col:
                    st.error("Parcel column not found in roll file.")
                    return None, {}
                target_width = _pick_target_width(_clean_pid_series(chunk[roll_col]))

                if (target_width or 0) > 0 and not tp_has_zf:
                    # Build a zero-filled TP key to match counties that pad to fixed width
                    df_tp["_PID_ZF_"] = df_tp["_PID_"].str.zfill(target_width)
                    tp_has_zf = True

            # Normalize roll parcel IDs for this chunk
            chunk["_PID_"] = _clean_pid_series(chunk[roll_col])

            # --- First pass: direct key match (_PID_ ↔ _PID_) ---
            m1 = chunk.merge(
                df_tp,
                on="_PID_",
                how="inner",
                suffixes=("", "_TP"),
                copy=False,
            )
            first_pass_count += len(m1)

            # --- Retry pass: zero-filled match (_PID_ ↔ _PID_ZF_) for unmatched rows ---
            m2 = pd.DataFrame()
            if tp_has_zf and (target_width or 0) > 0:
                # Only attempt retry for roll rows not already matched in m1
                matched_pids = set(m1["_PID_"].unique())
                retry_chunk = chunk.loc[~chunk["_PID_"].isin(matched_pids)].copy()

                if not retry_chunk.empty:
                    m2 = retry_chunk.merge(
                        df_tp,
                        left_on="_PID_",
                        right_on="_PID_ZF_",
                        how="inner",
                        suffixes=("", "_TP"),
                        copy=False,
                    )
                    retry_count += len(m2)

            # Combine this chunk's matches (include all roll + all TP columns)
            out = pd.concat([m1, m2], ignore_index=True)

            # Drop helper key if present from TP side to keep output tidy
            if "_PID_ZF_" in out.columns:
                out = out.drop(columns=["_PID_ZF_"])

            # Append to the output CSV
            write_header = not matches_path.exists()
            out.to_csv(
                matches_path,
                mode="a",
                header=write_header,
                index=False,
                quoting=csv.QUOTE_NONNUMERIC,
            )

    # Build preview + stats
    if matches_path.exists() and matches_path.stat().st_size > 0:
        preview_df = pd.read_csv(matches_path, dtype=str, nrows=50)
        stats = {
            "roll_rows": roll_rows_total,
            "tp_rows": len(df_tp),
            "first_pass_matches": first_pass_count,
            "retry_matches": retry_count,
            "combined_matches": first_pass_count + retry_count,
            "target_width": target_width or 0,
            "zip_name": file_item.get("Name"),
            "matches_path": matches_path,
        }
        return preview_df, stats

    return None, {}



# ============================================================
# Caching for dropdown options
# ============================================================
@st.cache_data(show_spinner=False, ttl=3600)
def list_2025p_zip_files_cached():
    all_items = list_all_2025p_files()
    zips = [f for f in all_items if (f.get("Name", "").lower().endswith(".zip"))]
    # sort by filename for a stable dropdown
    zips.sort(key=lambda f: (f.get("Name") or "").lower())
    return zips


# ============================================================
# UI
# ============================================================
st.title("Florida 2025P NAL + TaxProper Merge")
st.caption("Matches by Parcel ID, retries unmatched with leading zeros to the roll's typical width. Handles very large counties safely.")

# 1) Choose the roll ZIP from a live dropdown (no text input)
with st.spinner("Listing available 2025P ZIP files..."):
    zip_options = list_2025p_zip_files_cached()

selected_zip = st.selectbox(
    "Choose a 2025P county ZIP",
    options=zip_options,
    format_func=lambda f: f.get("Name", "Unknown file") if isinstance(f, dict) else str(f),
    index=0 if zip_options else None,
    placeholder="Select a ZIP file..."
)

# Optional manual refresh
if st.button("Refresh file list"):
    list_2025p_zip_files_cached.clear()
    st.rerun()

# 2) Upload TP CSV
tp_file = st.file_uploader("Upload TaxProper CSV", type="csv")

# 3) Run
if st.button("Run Merge"):
    if not tp_file or not selected_zip:
        st.error("Please upload your TaxProper CSV and choose a 2025P ZIP file.")
    else:
        tp_path = "temp_tp.csv"
        with open(tp_path, "wb") as f:
            f.write(tp_file.getbuffer())

        with st.spinner(f"Processing {selected_zip.get('Name', 'selected file')}..."):
            final_df, stats = do_merge_selected(tp_path, selected_zip)

        if final_df is not None:
            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("Roll rows", f"{stats['roll_rows']:,}")
            c2.metric("TaxProper rows", f"{stats['tp_rows']:,}")
            c3.metric("First-pass matches", f"{stats['first_pass_matches']:,}")
            c4.metric("Retry matches", f"{stats['retry_matches']:,}")
            c5.metric("Total matches", f"{stats['combined_matches']:,}")
            st.caption(f"2025P file: **{stats['zip_name']}** • Zero-fill width: **{stats['target_width']}**")

            st.dataframe(final_df, use_container_width=True)

            with open(stats["matches_path"], "rb") as f:
                full_csv_bytes = f.read()
            st.download_button(
                "Download FULL Results CSV",
                data=full_csv_bytes,
                file_name="merged_results_2025P.csv",
                mime="text/csv"
            )
        else:
            st.warning("No matches found or the selected county file did not yield results.")
