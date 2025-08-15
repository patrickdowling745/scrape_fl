import io
import re
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Tax Match (with Leading-Zero Retry)", layout="wide")

# -------------------------
# Helpers
# -------------------------
def normalize_series(
    s: pd.Series,
    *,
    strip_spaces: bool = True,
    to_upper: bool = True,
    keep_only_alnum: bool = False,
    keep_only_digits: bool = False,
):
    s = s.astype(str).fillna("")
    if strip_spaces:
        s = s.str.strip()
    if to_upper:
        s = s.str.upper()
    if keep_only_digits:
        s = s.apply(lambda x: re.sub(r"[^0-9]", "", x))
    elif keep_only_alnum:
        s = s.apply(lambda x: re.sub(r"[^0-9A-Z]", "", x))
    return s


def zfill_to_width(series: pd.Series, width: int) -> pd.Series:
    return series.astype(str).str.zfill(width)


def match_with_leading_zero_retry(
    left: pd.DataFrame,
    right: pd.DataFrame,
    left_key: str,
    right_key: str,
    how_first: str = "inner",
    normalize_opts: dict | None = None,
):
    """
    1) Normalize keys (same rules applied to both tables).
    2) First pass: merge on the normalized key.
    3) Find left-unmatched rows.
    4) ZFILL ONLY the left-unmatched keys to the max len of right keys.
    5) Second pass merge on the zfilled keys.
    6) Combine results + drop duplicates.
    """
    normalize_opts = normalize_opts or {}
    L = left.copy()
    R = right.copy()

    # Create normalized columns
    L["_norm_key"] = normalize_series(L[left_key], **normalize_opts)
    R["_norm_key"] = normalize_series(R[right_key], **normalize_opts)

    # First pass
    first = L.merge(R, left_on="_norm_key", right_on="_norm_key", how=how_first, suffixes=("_L", "_R"))

    # Figure out left rows that did NOT match
    matched_keys = set(first["_norm_key"].unique())
    left_unmatched = L[~L["_norm_key"].isin(matched_keys)].copy()

    # Determine a width for zero-fill from RIGHT side observed length distribution
    # Use the max length of right keys after normalization (fallback to current left lengths if empty)
    if len(R) > 0:
        target_width = int(R["_norm_key"].str.len().max())
        if pd.isna(target_width):
            target_width = int(left_unmatched["_norm_key"].str.len().max() or 0)
    else:
        target_width = int(left_unmatched["_norm_key"].str.len().max() or 0)

    # Second pass: only if we have something to zfill AND target width increases anything
    second = pd.DataFrame()
    if target_width > 0 and not left_unmatched.empty:
        left_unmatched["_norm_key_retry"] = zfill_to_width(left_unmatched["_norm_key"], target_width)

        # Only retry where zfill actually changed the value
        retry_subset = left_unmatched[left_unmatched["_norm_key_retry"] != left_unmatched["_norm_key"]].copy()
        if not retry_subset.empty:
            second = retry_subset.merge(
                R,
                left_on="_norm_key_retry",
                right_on="_norm_key",
                how="inner",
                suffixes=("_L", "_R"),
            )

    # Combine results
    combined = pd.concat([first, second], ignore_index=True)

    # Drop duplicate match rows by the key from each side, if present
    # Build a dedup key (prefer the right normalized key)
    if "_norm_key_R" in combined.columns:
        # when both exist by suffixing from merges
        dedup_key = combined["_norm_key_R"].fillna(combined["_norm_key"])
    else:
        dedup_key = combined["_norm_key"]

    combined = combined.loc[~dedup_key.duplicated(keep="first")].copy()

    # Compute final unmatched (after both passes)
    after_match_keys = set(dedup_key.dropna().unique())
    left_after = L[~L["_norm_key"].isin(after_match_keys)].copy()
    right_after = R[~R["_norm_key"].isin(after_match_keys)].copy()

    # Clean up helper columns for user display
    def cleanup(df: pd.DataFrame):
        cols = [c for c in df.columns if not c.startswith("_norm_key")]
        return df[cols]

    return {
        "first_pass_matches": cleanup(first),
        "retry_matches": cleanup(second),
        "combined_matches": cleanup(combined),
        "left_unmatched_after": cleanup(left_after),
        "right_unmatched_after": cleanup(right_after),
        "target_width": target_width,
    }


def to_csv_download(df: pd.DataFrame, filename: str) -> tuple[bytes, str]:
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue().encode(), filename


# -------------------------
# UI
# -------------------------
st.title("Match Records with Leading-Zero Retry")
st.caption("Upload two CSVs (e.g., TaxProper file on the left, County file on the right). "
           "Pick the key columns. We'll match, then retry unmatched rows by zero-filling.")

with st.sidebar:
    st.header("1) Upload Files")
    left_file = st.file_uploader("Left CSV (e.g., TaxProper)", type=["csv"], key="left")
    right_file = st.file_uploader("Right CSV (e.g., County)", type=["csv"], key="right")

    st.header("2) Normalization Options")
    strip_spaces = st.checkbox("Strip leading/trailing spaces", True)
    to_upper = st.checkbox("Uppercase keys", True)
    keep_only_digits = st.checkbox("Keep only digits (remove all non-digits)", False)
    keep_only_alnum = st.checkbox("Keep only A–Z and 0–9", False)
    st.caption("Tip: If your keys are parcel numbers that sometimes include dashes/spaces, "
               "turn on **Keep only digits**.")

    run_btn = st.button("Run match")

if left_file and right_file:
    left_df = pd.read_csv(left_file, dtype=str, low_memory=False)
    right_df = pd.read_csv(right_file, dtype=str, low_memory=False)

    st.subheader("Column Selection")
    col_left = st.selectbox("Left key column", left_df.columns, index=0)
    col_right = st.selectbox("Right key column", right_df.columns, index=0)

    if run_btn:
        opts = dict(
            strip_spaces=strip_spaces,
            to_upper=to_upper,
            keep_only_digits=keep_only_digits,
            keep_only_alnum=(keep_only_alnum and not keep_only_digits),
        )

        with st.spinner("Matching..."):
            res = match_with_leading_zero_retry(
                left_df, right_df, col_left, col_right, how_first="inner", normalize_opts=opts
            )

        # Metrics
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Left rows", f"{len(left_df):,}")
        c2.metric("Right rows", f"{len(right_df):,}")
        c3.metric("Combined matches", f"{len(res['combined_matches']):,}")
        c4.metric("Retry zfill width", res["target_width"])

        tabs = st.tabs([
            "Combined Matches", "First-Pass Matches", "Retry Matches",
            "Left Unmatched (after both)", "Right Unmatched (after both)"
        ])

        with tabs[0]:
            st.dataframe(res["combined_matches"], use_container_width=True)
            if len(res["combined_matches"]) > 0:
                data, name = to_csv_download(res["combined_matches"], "combined_matches.csv")
                st.download_button("Download Combined Matches", data, file_name=name, mime="text/csv")

        with tabs[1]:
            st.dataframe(res["first_pass_matches"], use_container_width=True)
            if len(res["first_pass_matches"]) > 0:
                data, name = to_csv_download(res["first_pass_matches"], "first_pass_matches.csv")
                st.download_button("Download First-Pass Matches", data, file_name=name, mime="text/csv")

        with tabs[2]:
            st.dataframe(res["retry_matches"], use_container_width=True)
            if len(res["retry_matches"]) > 0:
                data, name = to_csv_download(res["retry_matches"], "retry_matches.csv")
                st.download_button("Download Retry Matches", data, file_name=name, mime="text/csv")

        with tabs[3]:
            st.dataframe(res["left_unmatched_after"], use_container_width=True)
            if len(res["left_unmatched_after"]) > 0:
                data, name = to_csv_download(res["left_unmatched_after"], "left_unmatched_after.csv")
                st.download_button("Download Left Unmatched", data, file_name=name, mime="text/csv")

        with tabs[4]:
            st.dataframe(res["right_unmatched_after"], use_container_width=True)
            if len(res["right_unmatched_after"]) > 0:
                data, name = to_csv_download(res["right_unmatched_after"], "right_unmatched_after.csv")
                st.download_button("Download Right Unmatched", data, file_name=name, mime="text/csv")
else:
    st.info("Upload both CSVs in the sidebar to begin.")
