import streamlit as st
import os
import zipfile
import subprocess
import tempfile
from pathlib import Path
from playwright.sync_api import sync_playwright
import pandas as pd

# ---------- One-time setup (per Streamlit session) ----------
@st.cache_resource
def ensure_chromium():
    # Install the Chromium binary Playwright needs (do it once per session)
    subprocess.run(["python", "-m", "playwright", "install", "chromium"], check=True)
    return True

ensure_chromium()

# Temporary extraction directory (per run)
EXTRACT_DIR = Path("temp_extract")
EXTRACT_DIR.mkdir(exist_ok=True)

URL = (
    "https://floridarevenue.com/property/dataportal/Pages/default.aspx"
    "?path=/property/dataportal/Documents/PTO%20Data%20Portal/Tax%20Roll%20Data%20Files/NAL/2025P"
)

def launch_browser(p):
    # Hardened launch for Streamlit/Docker-ish environments
    return p.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--no-zygote",
            "--single-process",
        ],
        timeout=120_000,  # give Chromium more time to start
    )

def safe_read_csv(path):
    # Try utf-8 first, fall back to latin-1 if needed
    try:
        return pd.read_csv(path)
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="latin-1")

def scrape_and_merge(tp_file_path, county: str):
    merged_results = []

    with sync_playwright() as p:
        browser = launch_browser(p)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        # Load the page and wait for DOM
        page.goto(URL, wait_until="domcontentloaded", timeout=120_000)

        try:
            # Click the county link and capture the file download
            with page.expect_download(timeout=120_000) as download_info:
                # Be explicit: click an <a> that contains the county text
                page.locator("a", has_text=county).first.click()
            download = download_info.value

            # Save zip to a real file path (Download.path() may be None on some hosts)
            tmp_zip = Path(tempfile.gettempdir()) / f"{county}_nal.zip"
            download.save_as(str(tmp_zip))

            # Extract CSV from ZIP
            with zipfile.ZipFile(tmp_zip, "r") as zf:
                csv_candidates = [n for n in zf.namelist() if n.lower().endswith(".csv")]
                if not csv_candidates:
                    st.error(f"No CSV file found for {county}")
                    return None

                csv_name = csv_candidates[0]
                zf.extract(csv_name, path=EXTRACT_DIR)
                csv_path = EXTRACT_DIR / csv_name

            # Read datasets
            df_county = safe_read_csv(csv_path)
            df_tp = safe_read_csv(tp_file_path)

            # Normalize Parcel IDs
            if "PARCEL_ID" not in df_county.columns:
                st.error("Expected 'PARCEL_ID' column missing in county file.")
                return None
            if "Parcel ID" not in df_tp.columns:
                st.error("Expected 'Parcel ID' column missing in TaxProper file.")
                return None

            df_county["PARCEL_ID"] = (
                df_county["PARCEL_ID"].astype(str).str.replace("-", "", regex=False).str.strip()
            )
            df_tp["Parcel ID"] = (
                df_tp["Parcel ID"].astype(str).str.replace("-", "", regex=False).str.strip()
            )

            # First merge attempt
            merged_df = pd.merge(
                df_county, df_tp, left_on="PARCEL_ID", right_on="Parcel ID", how="inner"
            )

            # Two-pass merge: prepend '0' for previously unmatched TP rows
            if not merged_df.empty:
                matched_ids = set(merged_df["Parcel ID"])
                unmatched_tp = df_tp[~df_tp["Parcel ID"].isin(matched_ids)].copy()
            else:
                unmatched_tp = df_tp.copy()

            if not unmatched_tp.empty:
                unmatched_tp["Parcel ID"] = unmatched_tp["Parcel ID"].apply(lambda x: "0" + x)
                retry_merge = pd.merge(
                    df_county, unmatched_tp, left_on="PARCEL_ID", right_on="Parcel ID", how="inner"
                )
                if not retry_merge.empty:
                    merged_df = pd.concat([merged_df, retry_merge], axis=0, ignore_index=True)

            merged_results.append(merged_df)

        except Exception as e:
            # Streamlit Cloud redacts details in UI; still show what we can
            st.error(f"Error processing {county}: {e}")

        finally:
            # Cleanup Playwright
            context.close()
            browser.close()

    if merged_results:
        out = pd.concat(merged_results, axis=0, ignore_index=True)
        return out
    return None

# -------------------------
# Streamlit UI
# -------------------------
st.title("Florida Tax Roll Scraper + TaxProper Merge")

tp_file = st.file_uploader("Upload TaxProper CSV", type="csv")
county = st.text_input("Enter County Name (e.g., Clay, Pinellas)")

if st.button("Run Scraper"):
    if tp_file and county:
        tp_path = "temp_tp.csv"
        with open(tp_path, "wb") as f:
            f.write(tp_file.getbuffer())

        with st.spinner("Scraping and merging..."):
            final_df = scrape_and_merge(tp_path, county)

        if final_df is not None and not final_df.empty:
            st.success(f"Found {len(final_df)} matches!")
            st.dataframe(final_df.head(50))
            csv_data = final_df.to_csv(index=False).encode("utf-8")
            st.download_button("Download Results", csv_data, "merged_results.csv")
        else:
            st.warning("No matches found after attempting merge.")
    else:
        st.error("Please upload a TaxProper file and enter a county name.")
