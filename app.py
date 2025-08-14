import streamlit as st
import os
import zipfile
import subprocess
from playwright.sync_api import sync_playwright
import pandas as pd

# Ensure Chromium is installed on Streamlit Cloud
subprocess.run(["playwright", "install", "chromium"])

# Temporary extraction directory
extract_dir = "temp_extract"
os.makedirs(extract_dir, exist_ok=True)

def scrape_and_merge(tp_file_path, county):
    merged_results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        page.goto(
            "https://floridarevenue.com/property/dataportal/Pages/default.aspx"
            "?path=/property/dataportal/Documents/PTO%20Data%20Portal/Tax%20Roll%20Data%20Files/NAL/2025P"
        )

        try:
            with page.expect_download() as download_info:
                page.get_by_text(county).click()
            download = download_info.value
            zip_path = download.path()

            if zip_path:
                # Extract CSV from ZIP
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    csv_files = [f for f in zip_ref.namelist() if f.lower().endswith('.csv')]
                    if not csv_files:
                        st.error(f"No CSV file found for {county}")
                        return None
                    
                    csv_file_name = csv_files[0]
                    zip_ref.extract(csv_file_name, path=extract_dir)
                    csv_file_path = os.path.join(extract_dir, csv_file_name)

                # Read datasets
                df_county = pd.read_csv(csv_file_path)
                df_tp = pd.read_csv(tp_file_path)

                # Normalize Parcel IDs
                df_county['PARCEL_ID'] = df_county['PARCEL_ID'].astype(str).str.replace('-', '').str.strip()
                df_tp['Parcel ID'] = df_tp['Parcel ID'].astype(str).str.replace('-', '').str.strip()

                # First merge attempt
                merged_df = pd.merge(df_county, df_tp, left_on="PARCEL_ID", right_on="Parcel ID", how="inner")

                # Two-pass merge: prepend 0 only to unmatched TaxProper IDs
                unmatched_tp = df_tp[~df_tp['Parcel ID'].isin(merged_df['Parcel ID'])].copy()
                if not unmatched_tp.empty:
                    unmatched_tp['Parcel ID'] = unmatched_tp['Parcel ID'].apply(lambda x: '0' + x)
                    retry_merge = pd.merge(df_county, unmatched_tp, left_on="PARCEL_ID", right_on="Parcel ID", how="inner")
                    merged_df = pd.concat([merged_df, retry_merge], axis=0)

                merged_results.append(merged_df)

                # Cleanup
                os.remove(csv_file_path)
                os.remove(zip_path)

        except Exception as e:
            st.error(f"Error processing {county}: {e}")

        context.close()
        browser.close()

    if merged_results:
        return pd.concat(merged_results, axis=0)
    return None

# -------------------------
# Streamlit UI
# -------------------------
st.title("Florida Tax Roll Scraper + TaxProper Merge")

tp_file = st.file_uploader("Upload TaxProper CSV", type="csv")
county = st.text_input("Enter County Name (e.g., Clay, Pinellas)")

if st.button("Run Scraper"):
    if tp_file and county:
        tp_path = os.path.join("temp_tp.csv")
        with open(tp_path, "wb") as f:
            f.write(tp_file.getbuffer())

        with st.spinner("Scraping and merging..."):
            final_df = scrape_and_merge(tp_path, county)

        if final_df is not None and not final_df.empty:
            st.success(f"Found {len(final_df)} matches!")
            st.dataframe(final_df.head(50))
            csv_data = final_df.to_csv(index=False).encode('utf-8')
            st.download_button("Download Results", csv_data, "merged_results.csv")
        else:
            st.warning("No matches found after attempting merge.")
    else:
        st.error("Please upload a TaxProper file and enter a county name.")
