import streamlit as st
import pandas as pd

# ---------------------------
# Helper functions
# ---------------------------
def find_differences(df1, df2, key):
    df1 = df1.set_index(key)
    df2 = df2.set_index(key)

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
        for col in df1.columns:
            if col in df2.columns:
                val_a = row_a[col]
                val_b = row_b[col]
                if str(val_a) != str(val_b):
                    diffs[col] = (val_a, val_b)

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
    df_a = pd.read_csv(file_a)
    df_b = pd.read_csv(file_b)

    st.subheader("Uploaded Files Preview")
    st.write("**File A**")
    st.dataframe(df_a.head())
    st.write("**File B**")
    st.dataframe(df_b.head())

    # Select primary key
    common_cols = list(set(df_a.columns).intersection(set(df_b.columns)))
    primary_key = st.selectbox("Select Primary Key Column", common_cols)

    if primary_key:
        extra_in_a, extra_in_b, mismatches = find_differences(df_a, df_b, primary_key)

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
                styles = []
                if row["File A"] != row["File B"]:
                    styles = [""] + [""] + ["background-color: #ffcccc"] + ["background-color: #ffcccc"]
                else:
                    styles = [""] * len(row)
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
