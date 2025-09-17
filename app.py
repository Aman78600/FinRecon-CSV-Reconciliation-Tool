import streamlit as st
import pandas as pd

# ---------------------------
# Helper functions
# ---------------------------
def load_clean_csv(uploaded_file):
    """Load CSV and auto-detect the header row by scanning for 'Date' or known columns"""
    raw = uploaded_file.read().decode("utf-8").splitlines()
    header_row = None

    # Detect header row (look for a row that has 'Date' or 'Invoice')
    for i, line in enumerate(raw):
        if "Date" in line or "Invoice" in line:
            header_row = i
            break

    if header_row is None:
        header_row = 0  # fallback: assume first row is header

    df = pd.read_csv(uploaded_file, skiprows=header_row)
    # Drop completely empty rows/columns
    df = df.dropna(how="all").reset_index(drop=True)
    return df


def find_differences(df1, df2, key_a, key_b):
    df1 = df1.set_index(key_a)
    df2 = df2.set_index(key_b)

    # Extra rows
    extra_in_df1 = df1.loc[~df1.index.isin(df2.index)]
    extra_in_df2 = df2.loc[~df2.index.isin(df1.index)]

    # Common rows
    common_keys = df1.index.intersection(df2.index)
    mismatches = []

    for k in common_keys:
        row_a = df1.loc[k]
        row_b = df2.loc[k]

        diffs = {}
        for col_a in df1.columns:
            if col_a in df2.columns:
                val_a = str(row_a[col_a])
                val_b = str(row_b[col_a])
                if val_a != val_b:
                    diffs[col_a] = (val_a, val_b)

        if diffs:
            mismatches.append({"Primary Key": k, "Differences": diffs})

    return extra_in_df1, extra_in_df2, mismatches


# ---------------------------
# Streamlit App
# ---------------------------
st.set_page_config(page_title="FinRecon - CSV Reconciliation Tool", layout="wide")
st.title("📊 FinRecon – CSV Reconciliation Tool")

# Upload files
file_a = st.file_uploader("Upload File A (e.g., Tally Data)", type=["csv"])
file_b = st.file_uploader("Upload File B (e.g., GST-B Data)", type=["csv"])

if file_a and file_b:
    # Reset pointer before re-reading (since read() was used)
    file_a.seek(0)
    file_b.seek(0)

    df_a = load_clean_csv(file_a)
    file_a.seek(0)
    df_b = load_clean_csv(file_b)

    st.subheader("Uploaded Files Preview (Auto-cleaned)")
    st.write("**File A**")
    st.dataframe(df_a.head())
    st.write("**File B**")
    st.dataframe(df_b.head())

    # Select primary keys separately
    primary_key_a = st.selectbox("Select Primary Key Column in File A", df_a.columns)
    primary_key_b = st.selectbox("Select Primary Key Column in File B", df_b.columns)

    if primary_key_a and primary_key_b:
        extra_in_a, extra_in_b, mismatches = find_differences(df_a, df_b, primary_key_a, primary_key_b)

        st.subheader("Results")

        # Extra rows
        st.write("### Rows extra in File A")
        st.dataframe(extra_in_a.reset_index())

        st.write("### Rows extra in File B")
        st.dataframe(extra_in_b.reset_index())

        # Mismatches
        st.write("### Mismatched Rows (with highlights)")

        if mismatches:
            mismatch_rows = []
            for m in mismatches:
                key_val = m["Primary Key"]
                for col, (val_a, val_b) in m["Differences"].items():
                    mismatch_rows.append({
                        "Primary Key": key_val,
                        "Column": col,
                        "File A": val_a,
                        "File B": val_b
                    })

            mismatch_df = pd.DataFrame(mismatch_rows)

            # Highlight mismatched cells
            def highlight_mismatches(row):
                styles = [""] * len(row)
                if row["File A"] != row["File B"]:
                    styles[2] = "background-color: #ffcccc"  # File A cell
                    styles[3] = "background-color: #ffcccc"  # File B cell
                return styles

            st.dataframe(mismatch_df.style.apply(highlight_mismatches, axis=1))

            # Export results
            @st.cache_data
            def convert_df(df):
                return df.to_csv(index=False).encode("utf-8")

            csv_mismatch = convert_df(mismatch_df)
            st.download_button("📥 Download Mismatches CSV", csv_mismatch, "mismatches.csv", "text/csv")

        else:
            st.success("✅ No mismatches found between File A and File B.")
