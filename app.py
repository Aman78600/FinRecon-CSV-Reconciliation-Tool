import streamlit as st
import pandas as pd
import numpy as np
import io
import base64
from datetime import datetime
import re

# Configure page
st.set_page_config(
    page_title="CSV Reconciliation Tool",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better UI/UX
st.markdown("""
<style>
    .main-header {
        text-align: center;
        padding: 2rem 0;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        color: white;
        border-radius: 10px;
        margin-bottom: 2rem;
    }
    
    .step-container {
        background: white;
        padding: 1.5rem;
        border-radius: 10px;
        box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        margin: 1rem 0;
        border-left: 4px solid #667eea;
    }
    
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1rem;
        border-radius: 10px;
        text-align: center;
        margin: 0.5rem;
    }
    
    .success-box {
        background: #d4edda;
        color: #155724;
        padding: 1rem;
        border-radius: 5px;
        border: 1px solid #c3e6cb;
        margin: 1rem 0;
    }
    
    .warning-box {
        background: #fff3cd;
        color: #856404;
        padding: 1rem;
        border-radius: 5px;
        border: 1px solid #ffeaa7;
        margin: 1rem 0;
    }
    
    .error-box {
        background: #f8d7da;
        color: #721c24;
        padding: 1rem;
        border-radius: 5px;
        border: 1px solid #f5c6cb;
        margin: 1rem 0;
    }
    
    .dataframe {
        border: 1px solid #ddd;
        border-radius: 5px;
    }
    
    .step-number {
        background: #667eea;
        color: white;
        width: 30px;
        height: 30px;
        border-radius: 50%;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        margin-right: 10px;
        font-weight: bold;
    }
    
    .highlight-mismatch {
        background-color: #ffebee !important;
        color: #c62828 !important;
    }
</style>
""", unsafe_allow_html=True)

class CSVReconciliationTool:
    def __init__(self):
        if 'step' not in st.session_state:
            st.session_state.step = 1
        if 'file_a_processed' not in st.session_state:
            st.session_state.file_a_processed = None
        if 'file_b_processed' not in st.session_state:
            st.session_state.file_b_processed = None
        if 'results' not in st.session_state:
            st.session_state.results = None
    
    def detect_header_row(self, df):
        """Detect the actual header row in messy CSV data"""
        header_keywords = [
            'date', 'invoice', 'bill', 'particulars', 'amount', 'gst', 'tax',
            'number', 'no', 'description', 'quantity', 'rate', 'total', 'gstin',
            'voucher', 'ledger', 'account', 'debit', 'credit', 'balance', 'name',
            'party', 'customer', 'vendor', 'supplier', 'document', 'reference'
        ]
        
        max_keyword_matches = 0
        header_row_idx = 0
        
        # Check first 15 rows for header (most metadata should be within this range)
        check_rows = min(15, len(df))
        
        for idx in range(check_rows):
            try:
                row = df.iloc[idx]
                # Convert row to string and check for keywords
                row_text = ' '.join([str(cell).lower() for cell in row if pd.notna(cell) and str(cell).strip() != ''])
                
                # Count keyword matches
                keyword_matches = sum(1 for keyword in header_keywords if keyword in row_text)
                
                # Also check if row has a good number of non-empty values (likely a header)
                non_empty_count = sum(1 for cell in row if pd.notna(cell) and str(cell).strip() != '')
                
                # Score the row based on keywords and non-empty values
                score = keyword_matches * 2 + (non_empty_count * 0.1)
                
                if score > max_keyword_matches and keyword_matches >= 1:
                    max_keyword_matches = score
                    header_row_idx = idx
                    
            except Exception:
                continue
        
        return header_row_idx
    
    def preprocess_csv(self, df):
        """Clean and preprocess CSV data"""
        try:
            # Remove completely empty rows and columns
            df = df.dropna(how='all').dropna(axis=1, how='all')
            
            # Detect header row
            header_idx = self.detect_header_row(df)
            
            # Set header and remove metadata rows
            if header_idx > 0:
                df.columns = df.iloc[header_idx]
                df = df.iloc[header_idx + 1:]
            
            # Reset index
            df = df.reset_index(drop=True)
            
            # Remove any remaining empty rows
            df = df.dropna(how='all')
            
            # Clean column names and handle duplicates
            cleaned_columns = []
            column_counts = {}
            
            for col in df.columns:
                # Clean the column name
                clean_col = str(col).strip()
                
                # Handle empty or NaN column names
                if clean_col == '' or clean_col == 'nan' or clean_col == 'None':
                    clean_col = 'Unnamed_Column'
                
                # Handle duplicate column names
                if clean_col in column_counts:
                    column_counts[clean_col] += 1
                    clean_col = f"{clean_col}_{column_counts[clean_col]}"
                else:
                    column_counts[clean_col] = 0
                
                cleaned_columns.append(clean_col)
            
            df.columns = cleaned_columns
            
            # Remove any columns that are still completely empty after processing
            df = df.dropna(axis=1, how='all')
            
            return df
            
        except Exception as e:
            st.error(f"Error processing CSV: {str(e)}")
            return pd.DataFrame()  # Return empty dataframe on error
    
    def compare_dataframes(self, df_a, df_b, key_a, key_b, cols_a, cols_b):
        """Compare two dataframes and identify differences"""
        try:
            # Ensure key columns exist
            if key_a not in df_a.columns:
                st.error(f"Primary key '{key_a}' not found in File A!")
                return None
            if key_b not in df_b.columns:
                st.error(f"Primary key '{key_b}' not found in File B!")
                return None
            
            # Ensure comparison columns exist
            missing_cols_a = [col for col in cols_a if col not in df_a.columns]
            missing_cols_b = [col for col in cols_b if col not in df_b.columns]
            
            if missing_cols_a:
                st.error(f"Columns not found in File A: {missing_cols_a}")
                return None
            if missing_cols_b:
                st.error(f"Columns not found in File B: {missing_cols_b}")
                return None
            
            # Create comparison datasets
            df_a_compare = df_a[[key_a] + cols_a].copy()
            df_b_compare = df_b[[key_b] + cols_b].copy()
            
            # Clean the key columns - remove NaN values and convert to string
            df_a_compare = df_a_compare.dropna(subset=[key_a])
            df_b_compare = df_b_compare.dropna(subset=[key_b])
            
            df_a_compare[key_a] = df_a_compare[key_a].astype(str).str.strip()
            df_b_compare[key_b] = df_b_compare[key_b].astype(str).str.strip()
            
            # Rename key columns to match
            df_a_compare = df_a_compare.rename(columns={key_a: 'key'})
            df_b_compare = df_b_compare.rename(columns={key_b: 'key'})
            
            # Remove rows with empty keys
            df_a_compare = df_a_compare[df_a_compare['key'].str.strip() != '']
            df_b_compare = df_b_compare[df_b_compare['key'].str.strip() != '']
            
            # Find rows only in A
            only_in_a = df_a_compare[~df_a_compare['key'].isin(df_b_compare['key'])]
            
            # Find rows only in B  
            only_in_b = df_b_compare[~df_b_compare['key'].isin(df_a_compare['key'])]
            
            # Find common rows
            common_keys = set(df_a_compare['key']) & set(df_b_compare['key'])
            
            # Find mismatches in common rows
            mismatches = []
            for key in common_keys:
                try:
                    rows_a = df_a_compare[df_a_compare['key'] == key]
                    rows_b = df_b_compare[df_b_compare['key'] == key]
                    
                    if len(rows_a) > 0 and len(rows_b) > 0:
                        # Use first occurrence if there are duplicates
                        row_a = rows_a.iloc[0]
                        row_b = rows_b.iloc[0]
                        
                        # Compare selected columns
                        mismatch_found = False
                        mismatch_details = {'key': key}
                        
                        for col_a, col_b in zip(cols_a, cols_b):
                            val_a = row_a.get(col_a, '')
                            val_b = row_b.get(col_b, '')
                            
                            # Convert to string for comparison to handle different data types
                            val_a_str = str(val_a).strip() if pd.notna(val_a) else ""
                            val_b_str = str(val_b).strip() if pd.notna(val_b) else ""
                            
                            mismatch_details[f'{col_a}_A'] = val_a
                            mismatch_details[f'{col_b}_B'] = val_b
                            
                            if val_a_str != val_b_str:
                                mismatch_found = True
                        
                        if mismatch_found:
                            mismatches.append(mismatch_details)
                            
                except Exception as e:
                    st.warning(f"Error comparing key '{key}': {str(e)}")
                    continue
            
            return {
                'only_in_a': only_in_a,
                'only_in_b': only_in_b,
                'mismatches': pd.DataFrame(mismatches) if mismatches else pd.DataFrame(),
                'total_a': len(df_a_compare),
                'total_b': len(df_b_compare),
                'common_count': len(common_keys)
            }
            
        except Exception as e:
            st.error(f"Error during comparison: {str(e)}")
            return None
    
    def create_download_link(self, df, filename, file_format='csv'):
        """Create download link for dataframe"""
        if file_format == 'csv':
            csv = df.to_csv(index=False)
            b64 = base64.b64encode(csv.encode()).decode()
            href = f'<a href="data:file/csv;base64,{b64}" download="{filename}.csv">📥 Download {filename}</a>'
        return href
    
    def render_header(self):
        """Render the main header"""
        st.markdown("""
        <div class="main-header">
            <h1>📊 CSV Reconciliation Tool</h1>
            <p>Compare and reconcile financial CSV reports with ease</p>
        </div>
        """, unsafe_allow_html=True)
    
    def render_step_1(self):
        """Step 1: File Upload"""
        st.markdown("""
        <div class="step-container">
            <h2><span class="step-number">1</span>Upload CSV Files</h2>
            <p>Upload your two CSV files for comparison. The tool will automatically detect and clean headers.</p>
        </div>
        """, unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📄 File A")
            file_a = st.file_uploader("Choose first CSV file", type=['csv'], key='file_a')
            
            if file_a:
                try:
                    # Try to read with UTF-8 encoding first
                    try:
                        df_a_raw = pd.read_csv(file_a, encoding='utf-8')
                    except UnicodeDecodeError:
                        # Fallback to latin-1 encoding
                        file_a.seek(0)  # Reset file pointer
                        df_a_raw = pd.read_csv(file_a, encoding='latin-1')
                    except Exception as e:
                        # Try with error handling for malformed CSV
                        file_a.seek(0)  # Reset file pointer
                        df_a_raw = pd.read_csv(file_a, encoding='utf-8', on_bad_lines='skip')
                    
                    if len(df_a_raw) == 0:
                        st.error("❌ File A appears to be empty or unreadable")
                        return
                    
                    st.success(f"✅ Loaded {len(df_a_raw)} rows")
                    
                    # Preview raw data
                    with st.expander("Preview Raw Data"):
                        st.dataframe(df_a_raw.head(10))
                    
                    # Process data
                    df_a_clean = self.preprocess_csv(df_a_raw)
                    
                    if len(df_a_clean) == 0:
                        st.error("❌ File A could not be processed. Please check the file format.")
                        return
                    
                    st.session_state.file_a_processed = df_a_clean
                    
                    st.markdown('<div class="success-box">📋 File A processed successfully!</div>', unsafe_allow_html=True)
                    with st.expander("Preview Cleaned Data"):
                        st.dataframe(df_a_clean.head())
                        
                except Exception as e:
                    st.error(f"❌ Error loading File A: {str(e)}")
                    st.info("💡 Try saving your CSV with UTF-8 encoding or check for special characters.")
        
        with col2:
            st.subheader("📄 File B")
            file_b = st.file_uploader("Choose second CSV file", type=['csv'], key='file_b')
            
            if file_b:
                try:
                    # Try to read with UTF-8 encoding first
                    try:
                        df_b_raw = pd.read_csv(file_b, encoding='utf-8')
                    except UnicodeDecodeError:
                        # Fallback to latin-1 encoding
                        file_b.seek(0)  # Reset file pointer
                        df_b_raw = pd.read_csv(file_b, encoding='latin-1')
                    except Exception as e:
                        # Try with error handling for malformed CSV
                        file_b.seek(0)  # Reset file pointer
                        df_b_raw = pd.read_csv(file_b, encoding='utf-8', on_bad_lines='skip')
                    
                    if len(df_b_raw) == 0:
                        st.error("❌ File B appears to be empty or unreadable")
                        return
                    
                    st.success(f"✅ Loaded {len(df_b_raw)} rows")
                    
                    # Preview raw data
                    with st.expander("Preview Raw Data"):
                        st.dataframe(df_b_raw.head(10))
                    
                    # Process data
                    df_b_clean = self.preprocess_csv(df_b_raw)
                    
                    if len(df_b_clean) == 0:
                        st.error("❌ File B could not be processed. Please check the file format.")
                        return
                    
                    st.session_state.file_b_processed = df_b_clean
                    
                    st.markdown('<div class="success-box">📋 File B processed successfully!</div>', unsafe_allow_html=True)
                    with st.expander("Preview Cleaned Data"):
                        st.dataframe(df_b_clean.head())
                        
                except Exception as e:
                    st.error(f"❌ Error loading File B: {str(e)}")
                    st.info("💡 Try saving your CSV with UTF-8 encoding or check for special characters.")
        
        if st.session_state.file_a_processed is not None and st.session_state.file_b_processed is not None:
            if st.button("➡️ Next: Select Primary Keys", type="primary"):
                st.session_state.step = 2
                st.rerun()
    
    def render_step_2(self):
        """Step 2: Primary Key Selection"""
        st.markdown("""
        <div class="step-container">
            <h2><span class="step-number">2</span>Select Primary Keys</h2>
            <p>Choose the columns that will be used as primary keys for matching records between files.</p>
        </div>
        """, unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🔑 Primary Key for File A")
            key_a = st.selectbox(
                "Select primary key column:",
                options=st.session_state.file_a_processed.columns.tolist(),
                key='key_a'
            )
            
            if key_a:
                st.info(f"Selected: **{key_a}**")
                unique_vals_a = st.session_state.file_a_processed[key_a].nunique()
                st.metric("Unique Values", unique_vals_a)
        
        with col2:
            st.subheader("🔑 Primary Key for File B")
            key_b = st.selectbox(
                "Select primary key column:",
                options=st.session_state.file_b_processed.columns.tolist(),
                key='key_b'
            )
            
            if key_b:
                st.info(f"Selected: **{key_b}**")
                unique_vals_b = st.session_state.file_b_processed[key_b].nunique()
                st.metric("Unique Values", unique_vals_b)
        
        if key_a and key_b:
            col_back, col_next = st.columns([1, 1])
            with col_back:
                if st.button("⬅️ Back", type="secondary"):
                    st.session_state.step = 1
                    st.rerun()
            with col_next:
                if st.button("➡️ Next: Select Columns to Compare", type="primary"):
                    st.session_state.primary_key_a = key_a
                    st.session_state.primary_key_b = key_b
                    st.session_state.step = 3
                    st.rerun()
    
    def render_step_3(self):
        """Step 3: Column Selection for Comparison"""
        st.markdown("""
        <div class="step-container">
            <h2><span class="step-number">3</span>Select Columns to Compare</h2>
            <p>Choose which columns from each file you want to compare for mismatches.</p>
        </div>
        """, unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📊 Columns from File A")
            available_cols_a = [col for col in st.session_state.file_a_processed.columns 
                              if col != st.session_state.primary_key_a]
            cols_a = st.multiselect(
                "Select columns to compare:",
                options=available_cols_a,
                key='cols_a'
            )
        
        with col2:
            st.subheader("📊 Columns from File B")
            available_cols_b = [col for col in st.session_state.file_b_processed.columns 
                              if col != st.session_state.primary_key_b]
            cols_b = st.multiselect(
                "Select columns to compare:",
                options=available_cols_b,
                key='cols_b'
            )
        
        if cols_a and cols_b:
            if len(cols_a) != len(cols_b):
                st.markdown('<div class="warning-box">⚠️ Please select the same number of columns from both files for comparison.</div>', unsafe_allow_html=True)
            else:
                st.markdown('<div class="success-box">✅ Column mapping looks good!</div>', unsafe_allow_html=True)
                
                # Show column mapping
                st.subheader("🔄 Column Mapping")
                mapping_df = pd.DataFrame({
                    'File A Columns': cols_a,
                    'File B Columns': cols_b
                })
                st.dataframe(mapping_df, use_container_width=True)
                
                col_back, col_compare = st.columns([1, 1])
                with col_back:
                    if st.button("⬅️ Back", type="secondary"):
                        st.session_state.step = 2
                        st.rerun()
                with col_compare:
                    if st.button("🔍 Start Comparison", type="primary"):
                        st.session_state.compare_cols_a = cols_a
                        st.session_state.compare_cols_b = cols_b
                        st.session_state.step = 4
                        st.rerun()
    
    def render_step_4(self):
        """Step 4: Results and Analysis"""
        st.markdown("""
        <div class="step-container">
            <h2><span class="step-number">4</span>Reconciliation Results</h2>
            <p>Analysis complete! Review the comparison results below.</p>
        </div>
        """, unsafe_allow_html=True)
        
        # Perform comparison if not already done
        if st.session_state.results is None:
            with st.spinner("🔄 Analyzing data..."):
                results = self.compare_dataframes(
                    st.session_state.file_a_processed,
                    st.session_state.file_b_processed,
                    st.session_state.primary_key_a,
                    st.session_state.primary_key_b,
                    st.session_state.compare_cols_a,
                    st.session_state.compare_cols_b
                )
                st.session_state.results = results
        
        results = st.session_state.results
        
        # Display metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown(f"""
            <div class="metric-card">
                <h3>{results['total_a']}</h3>
                <p>Total Rows in File A</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown(f"""
            <div class="metric-card">
                <h3>{results['total_b']}</h3>
                <p>Total Rows in File B</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            st.markdown(f"""
            <div class="metric-card">
                <h3>{len(results['only_in_a'])}</h3>
                <p>Extra in File A</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            st.markdown(f"""
            <div class="metric-card">
                <h3>{len(results['mismatches'])}</h3>
                <p>Mismatches Found</p>
            </div>
            """, unsafe_allow_html=True)
        
        # Detailed Results
        tab1, tab2, tab3 = st.tabs(["🔍 Mismatches", "📄 Extra in File A", "📄 Extra in File B"])
        
        with tab1:
            if len(results['mismatches']) > 0:
                st.subheader("Rows with Mismatches")
                st.dataframe(results['mismatches'], use_container_width=True)
                
                # Download link for mismatches
                st.markdown(
                    self.create_download_link(results['mismatches'], 'mismatches'),
                    unsafe_allow_html=True
                )
            else:
                st.success("🎉 No mismatches found!")
        
        with tab2:
            if len(results['only_in_a']) > 0:
                st.subheader("Rows Only in File A")
                st.dataframe(results['only_in_a'], use_container_width=True)
                
                # Download link
                st.markdown(
                    self.create_download_link(results['only_in_a'], 'extra_in_file_a'),
                    unsafe_allow_html=True
                )
            else:
                st.info("No extra rows in File A")
        
        with tab3:
            if len(results['only_in_b']) > 0:
                st.subheader("Rows Only in File B")
                st.dataframe(results['only_in_b'], use_container_width=True)
                
                # Download link
                st.markdown(
                    self.create_download_link(results['only_in_b'], 'extra_in_file_b'),
                    unsafe_allow_html=True
                )
            else:
                st.info("No extra rows in File B")
        
        # Navigation buttons
        col_back, col_restart = st.columns([1, 1])
        with col_back:
            if st.button("⬅️ Back to Column Selection", type="secondary"):
                st.session_state.step = 3
                st.rerun()
        with col_restart:
            if st.button("🔄 Start New Comparison", type="primary"):
                # Clear session state
                for key in list(st.session_state.keys()):
                    del st.session_state[key]
                st.rerun()

def main():
    tool = CSVReconciliationTool()
    
    # Render header
    tool.render_header()
    
    # Sidebar navigation
    st.sidebar.title("📍 Navigation")
    st.sidebar.markdown(f"**Current Step:** {st.session_state.step}")
    
    steps = [
        "1️⃣ Upload Files",
        "2️⃣ Select Primary Keys", 
        "3️⃣ Choose Comparison Columns",
        "4️⃣ View Results"
    ]
    
    for i, step in enumerate(steps, 1):
        if i == st.session_state.step:
            st.sidebar.markdown(f"**➤ {step}**")
        elif i < st.session_state.step:
            st.sidebar.markdown(f"✅ {step}")
        else:
            st.sidebar.markdown(f"⏳ {step}")
    
    # Render current step
    if st.session_state.step == 1:
        tool.render_step_1()
    elif st.session_state.step == 2:
        tool.render_step_2()
    elif st.session_state.step == 3:
        tool.render_step_3()
    elif st.session_state.step == 4:
        tool.render_step_4()
    
    # Footer
    st.markdown("---")
    st.markdown("**CSV Reconciliation Tool** | Built with ❤️ using Streamlit")

if __name__ == "__main__":
    main()
