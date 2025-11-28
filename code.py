import streamlit as st
import pandas as pd
import numpy as np
import io
import zipfile
import time
from datetime import datetime
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.figure import Figure
from collections import defaultdict
import base64
import json
import os
from streamlit.runtime.scriptrunner import RerunException
from streamlit.runtime.scriptrunner import get_script_run_ctx
from requests_oauthlib import OAuth2Session
import requests

# Try optional libraries for advanced features
try:
    from scipy import stats
except Exception:
    stats = None
try:
    from sklearn.linear_model import LogisticRegression
    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing import StandardScaler
except Exception:
    LogisticRegression = None
try:
    from pptx import Presentation
    from pptx.util import Inches
except Exception:
    Presentation = None

# ==================== CONFIG ====================

# Standard column names for scoring subjects
STANDARD_SCORE_COLS = ["Mathematics", "English", "Science", "History", "Project Score", "Attendance"]

# Boolean value mappings for data cleaning
ALLOWED_BOOL_TRUE = {"yes","y","true","1","t"}
ALLOWED_BOOL_FALSE = {"no","n","false","0","f"}

# OAuth state persistence file
STATE_FILE = "oauth_state_store.json"

def _load_state():
    """Load OAuth state from persistent JSON file."""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {"states": {}, "auth": {"authenticated": False, "last_seen": time.time()}}

def _save_state(data):
    """Save OAuth state to persistent JSON file."""
    with open(STATE_FILE, "w") as f:
        json.dump(data, f)

# Page configuration
st.set_page_config(page_title="Enhanced Multi-Track Reporter", layout="wide")

# Initialize session state variables
if "_oauth_states" not in st.session_state:
    st.session_state._oauth_states = {}
    
if 'authenticated' not in st.session_state:
    state_data = _load_state()
    st.session_state.authenticated = state_data.get("auth", {}).get("authenticated", False)
    st.session_state.user = None
    st.session_state.last_seen = state_data.get("auth", {}).get("last_seen", time.time())
    # Session timeout: 10 minutes
    now = time.time()
    if now - st.session_state.last_seen > 600:
        st.session_state.authenticated = False

if '_oauth_processed' not in st.session_state:
    st.session_state._oauth_processed = False

# ==================== UTILITIES ====================

@st.cache_data(show_spinner=False)
def coerce_numeric(value):
    """Convert value to float, handling text variations (N/A, %, commas, etc.)."""
    if pd.isna(value):
        return np.nan
    if isinstance(value, str):
        v = value.strip()
        if v == "":
            return np.nan
        if v.lower() in {"n/a", "na", "waived", "waiver", "none", "null"}:
            return np.nan
        v = v.replace("%","").replace(",","")
        try:
            return float(v)
        except ValueError:
            return np.nan
    try:
        return float(value)
    except Exception:
        return np.nan


def default_standardize(colname):
    """Auto-detect and map column names to standard names (e.g., 'Maths' → 'Mathematics')."""
    c = str(colname).strip().lower()
    if "math" in c:
        return "Mathematics"
    if "english" in c:
        return "English"
    if "science" in c:
        return "Science"
    if "history" in c:
        return "History"
    if "attendance" in c:
        return "Attendance"
    if "project" in c and "score" in c:
        return "Project Score"
    if c in ("income","incomestudent","income_student","income_support"):
        return "IncomeStudent"
    if "cohort" in c:
        return "Cohort"
    if "pass" in c:
        return "Passed"
    if c in ("name","student","student_name","studentname"):
        return "StudentName"
    return None

@st.cache_data(show_spinner=False)
def clean_dataframe(df, user_map=None):
    """Clean & standardize dataframe: rename columns, trim whitespace, coerce types, normalize booleans."""
    df = df.copy()
    
    # Apply user-provided or auto-detected column mappings
    rename_map = {}
    if user_map is None:
        user_map = {}
    for col in df.columns:
        if col in user_map and user_map[col]:
            rename_map[col] = user_map[col]
        else:
            derived = default_standardize(col)
            if derived:
                rename_map[col] = derived
    if rename_map:
        df = df.rename(columns=rename_map)

    # Trim whitespace in text columns
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).str.strip()

    # Coerce score columns to numeric
    for c in STANDARD_SCORE_COLS:
        if c in df.columns:
            df[c] = df[c].apply(coerce_numeric)

    # Normalize Attendance: if max ≤ 1.01, assume it's decimal and convert to percentage
    if 'Attendance' in df.columns:
        df['Attendance'] = df['Attendance'].apply(coerce_numeric)
        if df['Attendance'].notna().any():
            max_val = df['Attendance'].max(skipna=True)
            if max_val <= 1.01:
                df['Attendance'] = df['Attendance'] * 100

    # Normalize booleans (yes/no, true/false, 1/0)
    if 'Passed' in df.columns:
        df['Passed'] = df['Passed'].apply(lambda x: True if str(x).strip().lower()[:1] == 'y' else (False if str(x).strip().lower()[:1] == 'n' else np.nan))
    if 'IncomeStudent' in df.columns:
        df['IncomeStudent'] = df['IncomeStudent'].apply(lambda x: True if str(x).strip().lower() in ALLOWED_BOOL_TRUE else (False if str(x).strip().lower() in ALLOWED_BOOL_FALSE else np.nan))

    # Ensure StudentName column exists
    if 'StudentName' not in df.columns:
        candidate = [c for c in df.columns if 'name' in str(c).lower()]
        if candidate:
            df = df.rename(columns={candidate[0]:'StudentName'})
        else:
            df['StudentName'] = np.nan

    # Drop fully empty columns
    df = df.dropna(axis=1, how='all')
    return df

@st.cache_data(show_spinner=False)
def merge_sheets(dfs: list):
    """Merge multiple dataframes and reorder columns (StudentName, Track, etc. first)."""
    merged = pd.concat(dfs, ignore_index=True, sort=False)
    
    # Priority column order
    cols = [c for c in ["StudentName","Track","Cohort","IncomeStudent","Passed"] if c in merged.columns]
    for c in STANDARD_SCORE_COLS:
        if c in merged.columns and c not in cols:
            cols.append(c)
    for c in merged.columns:
        if c not in cols:
            cols.append(c)
    merged = merged[cols]
    return merged

@st.cache_data(show_spinner=False)
def compute_basic_stats(df):
    """Calculate mean, median, std, count for each score column."""
    stats = {}
    for c in STANDARD_SCORE_COLS:
        if c in df.columns:
            a = df[c].dropna()
            stats[c] = {
                'mean': float(a.mean()) if len(a)>0 else np.nan,
                'median': float(a.median()) if len(a)>0 else np.nan,
                'std': float(a.std()) if len(a)>0 else np.nan,
                'count': int(a.count())
            }
    return stats

@st.cache_data(show_spinner=False)
def compute_income_comparison_stats(df):
    """Compare score statistics between income-support and non-income-support students."""
    if 'IncomeStudent' not in df.columns:
        return pd.DataFrame()

    df_income = df[df['IncomeStudent'] == True]
    df_no_income = df[df['IncomeStudent'] == False]

    if df_income.empty and df_no_income.empty:
        return pd.DataFrame()

    stats_income = compute_basic_stats(df_income) if not df_income.empty else {}
    stats_no_income = compute_basic_stats(df_no_income) if not df_no_income.empty else {}

    # Build comparison table
    combined = []
    for col in STANDARD_SCORE_COLS:
        inc = stats_income.get(col)
        no = stats_no_income.get(col)
        if inc or no:
            combined.append({
                "Subject": col,
                "Income Mean": float(inc["mean"]) if inc and inc.get("mean") is not None else np.nan,
                "No-Income Mean": float(no["mean"]) if no and no.get("mean") is not None else np.nan,
                "Income Count": int(inc["count"]) if inc and inc.get("count") is not None else 0,
                "No-Income Count": int(no["count"]) if no and no.get("count") is not None else 0,
            })

    if not combined:
        return pd.DataFrame()
    out = pd.DataFrame(combined)
    out = out[["Subject", "Income Mean", "No-Income Mean", "Income Count", "No-Income Count"]]
    return out

@st.cache_data(show_spinner=False)
def compute_track_stats(df):
    """Compute aggregate statistics per track (avg, median, std for each subject)."""
    rows = []
    for t in df['Track'].dropna().unique():
        sub = df[df['Track']==t]
        r = {"Track": t, "NumStudents": len(sub)}
        for c in STANDARD_SCORE_COLS:
            if c in sub.columns:
                r[f"Avg_{c}"] = round(sub[c].mean(skipna=True),2)
                r[f"Med_{c}"] = round(sub[c].median(skipna=True),2)
                r[f"Std_{c}"] = round(sub[c].std(skipna=True),2)
        if 'Passed' in sub.columns:
            r['PassRate (%)'] = round(sub['Passed'].mean(skipna=True)*100,2)
        rows.append(r)
    return pd.DataFrame(rows)


def narrative_summary(df, track=None):
    """Generate a text summary: best/worst subject, pass rate, anomalies."""
    parts = []
    if track:
        df = df[df['Track']==track]
        parts.append(f"Summary for track {track}:")
    else:
        parts.append("Global summary:")
    
    # Best and worst subjects by mean score
    means = {c: df[c].mean(skipna=True) for c in STANDARD_SCORE_COLS if c in df.columns}
    if means:
        best = max(means.items(), key=lambda x: (np.nan_to_num(x[1], -9999)) )
        worst = min(means.items(), key=lambda x: (np.nan_to_num(x[1], 9999)) )
        parts.append(f"Best subject: {best[0]} ({best[1]:.2f}).")
        parts.append(f"Weakest subject: {worst[0]} ({worst[1]:.2f}).")
    
    # Overall pass rate
    if 'Passed' in df.columns:
        pr = df['Passed'].mean(skipna=True)
        parts.append(f"Average pass rate: {pr*100:.1f}%.")
    
    # Detect outliers using IQR
    high_outliers = []
    for c in STANDARD_SCORE_COLS:
        if c in df.columns:
            q3 = df[c].quantile(0.75)
            iqr = df[c].quantile(0.75) - df[c].quantile(0.25)
            out = df[df[c] > q3 + 1.5*iqr]
            if len(out)>0:
                high_outliers.append((c,len(out)))
    if high_outliers:
        parts.append("Anomalies detected in: " + ", ".join([f"{c} ({n})" for c,n in high_outliers]))
    
    return " ".join(parts)

def auto_download(data, file_name, mime):
    """Generate a download link if authenticated, else show warning."""
    if st.session_state.authenticated == True:
        b64 = base64.b64encode(data).decode()
        href = f'<a href="data:{mime};base64,{b64}" download="{file_name}">📁 Download {file_name}</a>'
        st.markdown(href, unsafe_allow_html=True)
    else:
        st.toast("Not authenticated: use 'admin' password",icon="🚨",duration="long")

import plotly.express as px
def distribution(merged, col):
    """Create a histogram for a given column."""
    fig = px.histogram(merged, x=col)
    return fig


def impute_missing(df, numeric_strategy='median', categorical_strategy='mode'):
    """Fill missing values: median for numbers, mode for categories. Return imputed df and report."""
    df = df.copy()
    report = {'numeric_imputed': {}, 'categorical_imputed': {}}
    num_cols = df.select_dtypes(include=['number']).columns.tolist()
    cat_cols = df.select_dtypes(include=['object','category','bool']).columns.tolist()
    
    if numeric_strategy == 'median':
        for c in num_cols:
            med = df[c].median(skipna=True)
            report['numeric_imputed'][c] = int(df[c].isna().sum())
            df.fillna({c: med}, inplace=True)
    
    if categorical_strategy == 'mode':
        for c in cat_cols:
            if df[c].isna().any():
                mode = df[c].mode(dropna=True)
                fill = mode.iloc[0] if len(mode)>0 else ''
                report['categorical_imputed'][c] = int(df[c].isna().sum())
                pd.set_option('future.no_silent_downcasting', True)
                df.fillna({c:fill}, inplace=True)
    
    return df, report

def detect_outliers(df, cols=None, method='zscore', thresh=3.0):
    """Flag outliers using z-score or IQR method."""
    if cols is None:
        cols = [c for c in STANDARD_SCORE_COLS if c in df.columns]
    numeric = df[cols].select_dtypes(include=['number'])
    is_out = pd.Series(False, index=df.index)
    
    if method == 'zscore':
        z = (numeric - numeric.mean())/numeric.std(ddof=0)
        is_out = (z.abs() > thresh).any(axis=1)
    elif method == 'iqr':
        Q1 = numeric.quantile(0.25)
        Q3 = numeric.quantile(0.75)
        IQR = Q3 - Q1
        lower = Q1 - thresh * IQR
        upper = Q3 + thresh * IQR
        is_out = ((numeric < lower) | (numeric > upper)).any(axis=1)
    
    return is_out

def cronbach_alpha(df, cols):
    """Calculate Cronbach's alpha (internal consistency measure)."""
    X = df[cols].dropna()
    k = X.shape[1]
    if k < 2 or X.shape[0] < 2:
        return None
    item_vars = X.var(axis=0, ddof=1).sum()
    total_var = X.sum(axis=1).var(ddof=1)
    alpha = (k / (k-1)) * (1 - item_vars / total_var) if total_var > 0 else None
    return alpha

def anova_and_eta_squared(df, value_col, group_col='Track'):
    """One-way ANOVA with effect size (eta-squared)."""
    if group_col not in df.columns or value_col not in df.columns:
        return {'f': None, 'p': None, 'eta2': None, 'note': 'missing columns'}
    
    grouped = [g.dropna().astype(float) for _, g in df.groupby(group_col)[value_col]]
    grouped = [g for g in grouped if len(g) > 0]
    if len(grouped) < 2:
        return {'f': None, 'p': None, 'eta2': None, 'note': 'need >=2 groups with data'}

    # Use scipy if available, else manual computation
    try:
        if stats is not None:
            f, p = stats.f_oneway(*grouped)
        else:
            raise ImportError
    except Exception:
        # Manual ANOVA computation
        all_vals = pd.concat(grouped, ignore_index=True)
        grand_mean = all_vals.mean()
        ss_between = sum(len(g) * (g.mean() - grand_mean) ** 2 for g in grouped)
        ss_within = sum(((g - g.mean()) ** 2).sum() for g in grouped)
        k = len(grouped)
        n = len(all_vals)
        df_between = k - 1
        df_within = n - k
        ms_between = ss_between / df_between if df_between > 0 else None
        ms_within = ss_within / df_within if df_within > 0 else None
        f = (ms_between / ms_within) if (ms_between is not None and ms_within not in (None, 0)) else None
        p = None

    # Calculate eta-squared (effect size)
    try:
        eta2 = float(ss_between / (ss_between + ss_within)) if (ss_between + ss_within) > 0 else None
    except Exception:
        all_df = df[[group_col, value_col]].dropna()
        grand = all_df[value_col].mean()
        groups2 = [g for _, g in all_df.groupby(group_col)[value_col]]
        ss_between = sum(len(g) * (g.mean() - grand) ** 2 for g in groups2)
        ss_within = sum(((g - g.mean()) ** 2).sum() for g in groups2)
        eta2 = float(ss_between / (ss_between + ss_within)) if (ss_between + ss_within) > 0 else None

    return {'f': f, 'p': p, 'eta2': eta2, 'groups': len(grouped)}

def kmeans_profiles(df, cols=None, n_clusters=3, random_state=0):
    """K-means clustering to identify student profiles. Returns clustered data and cluster centers."""
    from sklearn.cluster import KMeans
    from sklearn.preprocessing import StandardScaler
    if cols is None:
        cols = [c for c in STANDARD_SCORE_COLS if c in df.columns]
    X = df[cols].dropna()
    if X.shape[0] < n_clusters:
        return None
    scaler = StandardScaler()
    Xs = scaler.fit_transform(X)
    km = KMeans(n_clusters=n_clusters, random_state=random_state)
    labels = km.fit_predict(Xs)
    centers = scaler.inverse_transform(km.cluster_centers_)
    out_df = X.copy()
    out_df['cluster'] = labels
    return out_df, pd.DataFrame(centers, columns=cols)

def pca_transform(df, cols=None, n_components=2):
    """PCA dimensionality reduction for visualization."""
    from sklearn.decomposition import PCA
    from sklearn.preprocessing import StandardScaler
    if cols is None:
        cols = [c for c in STANDARD_SCORE_COLS if c in df.columns]
    X = df[cols].dropna()
    if X.shape[0] < 2:
        return None
    scaler = StandardScaler()
    Xs = scaler.fit_transform(X)
    pca = PCA(n_components=n_components, random_state=0)
    comps = pca.fit_transform(Xs)
    res = pd.DataFrame(comps, index=X.index, columns=[f'PC{i+1}' for i in range(n_components)])
    return res, pca.explained_variance_ratio_

# ==================== STREAMLIT MAIN PAGE ====================

st.title("Enhanced Multi-Track Data Reporter")

# ==================== AUTHENTICATION (OAuth2 Google) ====================

def _get_oauth_config():
    """Fetch OAuth provider configs from Streamlit secrets."""
    cfg = {
        "google": {
            "name": "Google",
            "authorize_url": "https://accounts.google.com/o/oauth2/v2/auth",
            "token_url": "https://oauth2.googleapis.com/token",
            "userinfo_url": "https://openidconnect.googleapis.com/v1/userinfo",
            "scope": ["openid","email","profile"]
        },
    }
    secrets_cfg = st.secrets.get("oauth", {}) if hasattr(st, "secrets") else {}
    for k,v in secrets_cfg.items():
        cfg[k] = {**cfg.get(k, {}), **v}
    return cfg

def _redirect_uri():
    """Get OAuth redirect URI from secrets or default to localhost."""
    return st.secrets.get("oauth_redirect_uri") if hasattr(st, "secrets") and st.secrets.get("oauth_redirect_uri") else "http://localhost:8501/"

def start_oauth_flow(provider_key, client_id, authorize_url, scope):
    """Initiate OAuth login flow by redirecting to provider."""
    redirect_uri = _redirect_uri()
    oauth = OAuth2Session(client_id=client_id, scope=scope, redirect_uri=redirect_uri)
    auth_url, state = oauth.authorization_url(authorize_url)
    
    # Store state for validation on callback
    data = _load_state()
    if "states" not in data:
        data["states"] = {}
    data["states"][state] = {"provider": provider_key, "ts": time.time()}
    auth = data.get("auth", {})
    auth["authenticated"] = True  # Simplified: assumes auth success (10min timeout applied)
    auth["user"] = provider_key
    auth["last_seen"] = time.time()
    _save_state(data)
    
    st.session_state.authenticated = True
    st.session_state.user = {"provider": provider_key, "info": {"email": "unknown"}}
    
    # Redirect to provider
    st.write(
        f'<meta http-equiv="refresh" content="0; url={auth_url}" />',
        unsafe_allow_html=True
    )


def finish_oauth_flow(provider_cfg, client_id, client_secret, token_url, userinfo_url):
    """Complete OAuth callback: exchange code for token, fetch user info."""
    params = st.query_params

    # Extract callback params
    state = params.get("state")
    if isinstance(state, list):
        state = state[0]
    code = params.get("code", [None])[0] if isinstance(params.get("code"), list) else params.get("code")
    if isinstance(code, list):
        code = code[0]

    if not state:
        st.error("State param missing in callback")
        return False

    # Validate state against persistent store
    state_store = _load_state()
    store_entry = state_store.get("states", {}).get(state)

    if not store_entry:
        st.error("OAuth state not found. This usually happens when the callback arrives in a new session.")
        return False

    # Exchange authorization code for token
    redirect_uri = _redirect_uri()
    oauth = OAuth2Session(client_id=client_id, state=state, redirect_uri=redirect_uri)

    try:
        token = oauth.fetch_token(
            token_url,
            client_secret=client_secret,
            code=code,
            include_client_id=True
        )
    except Exception as e:
        st.error(f"Token exchange failed: {e}")
        if state in state_store.get("states", {}):
            del state_store["states"][state]
            _save_state(state_store)
        return False

    # Fetch authenticated user info
    try:
        resp = requests.get(
            userinfo_url,
            headers={"Authorization": f"Bearer {token.get('access_token')}"},
            timeout=10
        )
        userinfo = resp.json() if resp.ok else {"sub": None}
    except Exception:
        userinfo = {"sub": None}

    # Cleanup state and update session
    if state in state_store.get("states", {}):
        del state_store["states"][state]
        _save_state(state_store)

    st.session_state.authenticated = True
    st.session_state.user = {
        "provider": store_entry.get('provider'),
        "info": userinfo
    }
    st.success(f"Authenticated as {userinfo.get('email') or userinfo.get('name') or 'user'}")
    return True

# Check for OAuth callback
params = st.query_params
if params.get("code") and params.get("state"):
    state_param = params.get("state")
    if isinstance(state_param, list):
        state_param = state_param[0]
    
    oauth_cfg = _get_oauth_config()
    state_store = _load_state()
    store_entry = state_store.get("states", {}).get(state_param)
    
    if store_entry:
        provider_key = store_entry.get('provider')
        cfg = oauth_cfg.get(provider_key)
        if cfg:
            secret_block = (st.secrets.get("oauth", {}) if hasattr(st, "secrets") else {}).get(provider_key, {})
            client_id = secret_block.get("client_id")
            client_secret = secret_block.get("client_secret")
            if client_id:
                finished = finish_oauth_flow(cfg, client_id, client_secret, cfg["token_url"], cfg.get("userinfo_url"))
                if finished:
                    st.query_params.clear()
                    time.sleep(0.5)
                    st.rerun()
    else:
        st.query_params.clear()

# Sidebar: Authentication controls
with st.sidebar:
    st.header("Connection (optional)")
    oauth_cfg = _get_oauth_config()
    shown = False
    for key, cfg in oauth_cfg.items():
        secret_block = (st.secrets.get("oauth", {}) if hasattr(st, "secrets") else {}).get(key, {})
        client_id = secret_block.get("client_id")
        client_secret = secret_block.get("client_secret")
        if client_id:
            shown = True
            if st.session_state.authenticated == False:
                # Show login button
                if st.button(f"Start login with {cfg.get('name', key).capitalize()}", key=f"btn_oauth_{key}"):
                    start_oauth_flow(key, client_id, cfg["authorize_url"], cfg.get("scope", ["openid","email"]))
            else:
                # Show session info and expiry timer
                time_left = 600 - (time.time() - st.session_state.last_seen)
                st.success(f"Connected via {cfg.get('name', key).capitalize()} : expires in {int(time_left)//60}m {int(time_left)%60}s")
    
    if not shown:
        st.info("No OAuth providers configured. To enable SSO store provider configs in Streamlit secrets under 'oauth'.\n\nFallback: local password (not recommended for production).")
        pw = st.text_input("Simple password to activate exports", type='password', key="fallback_pw")
        if pw and pw == "admin":
            st.session_state.authenticated = True
            st.success("Authenticated — exports activated")
        elif pw:
            st.error("Incorrect password—restricted features")


# ==================== FILE UPLOAD ====================

st.sidebar.header("Upload (multiple files accepted)")
files = st.sidebar.file_uploader("Upload Excel (.xlsx/.xls) or CSV (multiple)", type=['xlsx','xls','csv'], accept_multiple_files=True)

# Reset OAuth processed flag if files change
if files and st.session_state.get('_last_file_count') != len(files):
    st.session_state._oauth_processed = False
    st.session_state._last_file_count = len(files)

if not files:
    st.info("Upload at least one Excel or CSV file to get started.")
    st.stop()

# ==================== READ & CLEAN FILES ====================

sheets = {}
manual_maps = {}
clean_reports = {}

for uploaded in files:
    name = uploaded.name
    if name.lower().endswith(('.xlsx','.xls')):
        try:
            # Read Excel file with multiple sheets
            xls = pd.ExcelFile(uploaded)
            for sheet in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=sheet, engine='openpyxl' if name.lower().endswith('.xlsx') else None)
                df['Track'] = sheet
                sheets[f"{name}::{sheet}"] = df
        except Exception:
            try:
                # Fallback: read entire Excel as single table
                df = pd.read_excel(uploaded)
                df['Track'] = name
                sheets[name] = df
            except Exception as e:
                st.error(f"Cannot read {name}: {e}")
    else:
        try:
            # Read CSV file
            df = pd.read_csv(uploaded)
            df['Track'] = name
            sheets[name] = df
        except Exception as e:
            st.error(f"Cannot read {name}: {e}")

st.sidebar.success(f"Loaded {len(sheets)} tables from uploads")

# Sidebar: Show sheets and allow manual column mapping
st.sidebar.header("Automatic & manual column mapping")
for key, df in sheets.items():
    st.sidebar.markdown(f"**{key}** — {df.shape[0]} rows x {df.shape[1]} cols")
    if st.sidebar.checkbox(f"See columns: {key}", key=f"cols_{key}"):
        st.sidebar.write(list(df.columns))
    # Manual column mapping for each sheet
    with st.sidebar.expander(f"Manual mapping: {key}"):
        user_map = {}
        for col in df.columns:
            suggested = default_standardize(col) or ""
            newname = st.text_input(f"Map {col} →", value=suggested, key=f"map_{key}_{col}\n")
            if newname:
                user_map[col] = newname
        manual_maps[key] = user_map

# Clean each sheet and generate cleaning report
cleaned_list = []
for key, df in sheets.items():
    cm = manual_maps.get(key, {})
    cleaned = clean_dataframe(df, user_map=cm)
    # Ensure Track column
    if 'Track' not in cleaned.columns:
        cleaned['Track'] = key
    else:
        cleaned['Track'] = cleaned['Track'].fillna(key)
    cleaned_list.append(cleaned)
    # Generate cleaning report
    report = {
        'original_columns': list(df.columns),
        'mapped_columns': {k:v for k,v in cm.items() if v},
        'rows': int(df.shape[0]),
        'cols_after': int(cleaned.shape[1])
    }
    clean_reports[key] = report

# Merge all cleaned sheets
merged = merge_sheets(cleaned_list)
st.success(f"Merged dataset with {len(merged)} rows and {merged.shape[1]} cols")

# Show cleaning reports
with st.expander("Cleaning reports (per table)", expanded=False):
    for k,r in clean_reports.items():
        st.write(f"**{k}** — rows: {r['rows']} — cols after: {r['cols_after']}")
        if r['mapped_columns']:
            st.write("Automatic mappings applied:")
            st.json(r['mapped_columns'])

# ==================== DATA QUALITY ====================

def quality_score(df):
    """Compute data quality score (0-100): penalize missing data and missing IDs."""
    total = df.size
    missing = df.isna().sum().sum()
    pct_missing = missing/total if total>0 else 0
    distinct_ids = df['StudentName'].nunique() if 'StudentName' in df.columns else 0
    score = max(0, 100 - pct_missing*100 - (0 if distinct_ids>0 else 20))
    return round(score,1), pct_missing

qs, pct_missing = quality_score(merged)
st.metric('Quality score', f"{qs}/100")
st.info(f"Percentage missing (cells): {pct_missing*100:.1f}%")

if st.checkbox("Show missing-data imputation report"):
    imputed, imp_report = impute_missing(merged)
    st.json(imp_report)
    st.write("After imputation sample:")
    st.dataframe(imputed.head())

# Hide columns with >95% missing values
if st.checkbox('Hide columns with >95% missing values'):
    hide_thresh = 0.95
    keep = [c for c in merged.columns if merged[c].isna().mean() <= hide_thresh]
    merged = merged[keep]

# ==================== DUPLICATE DETECTION ====================

st.subheader("Duplicate detection")

# Default duplicate column: StudentID or StudentName
default_dup_col = "StudentID" if "StudentID" in merged.columns else (
    "StudentName" if "StudentName" in merged.columns else None
)

dup_cols = st.multiselect(
    "Column to use to detect duplicates",
    options=list(merged.columns),
    default=[default_dup_col] if default_dup_col else []
)

# Get duplicates
if dup_cols:
    dups = merged[merged.duplicated(subset=dup_cols, keep=False)]
else:
    dups = pd.DataFrame(columns=merged.columns)

st.write(f"Number of duplicates detected : {len(dups)}")
st.dataframe(dups)

# Remove duplicates option
if dup_cols and len(dups) > 0:
    if st.button("Remove duplicates (keep the first record)"):
        merged = merged.drop_duplicates(subset=dup_cols, keep='first').reset_index(drop=True)
        st.success("Duplicates removed")
else:
    st.info("No duplicate detected according to the chosen columns or no selected column.")

# ==================== ADVANCED STATISTICS ====================

st.header("Advanced statistics")
col1, col2 = st.columns([1,2])
with col1:
    st.metric("Total students", len(merged))
    st.metric("Distinct tracks", merged['Track'].nunique())
    if 'Cohort' in merged.columns:
        st.metric("Distinct cohorts", merged['Cohort'].nunique())
with col2:
    basic = compute_basic_stats(merged)
    st.write("Averages / Medians / Std (excerpt)")
    st.table(pd.DataFrame(basic).T)

# Track-level summary table
st.subheader("Track-level summary")
track_stats = compute_track_stats(merged)
st.dataframe(track_stats)

# Income support comparison
st.subheader("Performance comparison: Income vs No-Income")

if 'IncomeStudent' not in merged.columns:
    st.info("The 'IncomeStudent' column is not present in the dataset.")
else:
    df_compare = compute_income_comparison_stats(merged)

    if not df_compare.empty:
        st.dataframe(df_compare)

        fig = px.bar(
            df_compare,
            x="Subject",
            y=["Income Mean", "No-Income Mean"],
            barmode="group",
            title="Average Scores: Income vs Not Income",
        )
        st.plotly_chart(fig, width='stretch')
    else:
        st.info("Not enough data to compute Income vs No-Income statistics.")

# ==================== VISUALIZATIONS ====================

st.subheader('Interactive visualisations')

# Correlation heatmap
if st.checkbox("Display the courses correlation matrix"):
    numeric_cols = [c for c in STANDARD_SCORE_COLS if c in merged.columns]
    corr = merged[numeric_cols].corr()
    fig, ax = plt.subplots()
    cax = ax.matshow(corr)
    fig.colorbar(cax)
    ax.set_xticks(range(len(numeric_cols)))
    ax.set_xticklabels(numeric_cols, rotation=45)
    ax.set_yticks(range(len(numeric_cols)))
    ax.set_yticklabels(numeric_cols)
    st.pyplot(fig)

# Distribution histogram
if st.checkbox('Show distribution'):
    numeric_cols = [c for c in STANDARD_SCORE_COLS if c in merged.columns]
    sel = st.selectbox('Select column', options=numeric_cols)
    fig = distribution(merged, sel)
    st.plotly_chart(fig)

# Track comparison
st.subheader("Track comparator")
tracks = list(merged['Track'].unique())
if len(tracks) >= 2:
    t1 = st.selectbox("Track A", options=tracks, index=0, key='comp_a')
    t2 = st.selectbox("Track B", options=tracks, index=1, key='comp_b')
    if t1 and t2 and t1!=t2:
        a = merged[merged['Track']==t1]
        b = merged[merged['Track']==t2]
        comp = {}
        for c in STANDARD_SCORE_COLS:
            if c in merged.columns:
                comp[c] = {'A_mean': a[c].mean(), 'B_mean': b[c].mean(), 'diff': a[c].mean()-b[c].mean()}
        st.table(pd.DataFrame(comp).T)

# Student search
st.subheader("Student research")
search_name = st.text_input("Enter the name of a student (partial ok)")
if search_name:
    found = merged[(merged['StudentName']+" "+merged['LastName']).str.contains(search_name, case=False, na=False)]
    if len(found)>0:
        st.dataframe(found)
    else:
        st.info("No students found")

# Radar chart for track profile
st.subheader("Radar: average profile by track")
if st.checkbox("Display radar for a track"):
    rt = st.selectbox("Radar for tracks", options=tracks, key='radar_track')
    df_rt = merged[merged['Track']==rt]
    cats = [c for c in STANDARD_SCORE_COLS if c in df_rt.columns]
    vals = [df_rt[c].mean(skipna=True) for c in cats]
    if len(cats)>=3:
        angles = np.linspace(0, 2*np.pi, len(cats), endpoint=False).tolist()
        vals += vals[:1]
        angles += angles[:1]
        fig = Figure(figsize=(6,6))
        ax = fig.add_subplot(111, polar=True)
        ax.plot(angles, vals, 'o-')
        ax.fill(angles, vals, alpha=0.25)
        ax.set_thetagrids(np.degrees(angles[:-1]), cats)
        st.pyplot(fig)
    else:
        st.info("Not enough courses for a radar (>=3 required)")

# ==================== ADVANCED ANALYSES ====================

st.subheader("Additional analyses")

if st.checkbox("Detect outliers (z-score)"):
    outmask = detect_outliers(merged)
    st.write(f"Outliers detected: {int(outmask.sum())}")
    st.dataframe(merged.loc[outmask].head())

if st.checkbox("Cronbach's alpha for course scores"):
    cols = [c for c in STANDARD_SCORE_COLS if c in merged.columns]
    alpha = cronbach_alpha(merged, cols)
    st.metric("Cronbach's alpha", f"{alpha:.3f}" if alpha is not None else "N/A")

if st.checkbox("ANOVA across tracks (select subject)"):
    if 'Track' not in merged.columns:
        st.info("Need a 'Track' column to compare groups.")
    else:
        subj_options = [c for c in STANDARD_SCORE_COLS if c in merged.columns]
        if not subj_options:
            subj_options = [c for c in merged.columns if pd.api.types.is_numeric_dtype(merged[c])]
        if not subj_options:
            st.info("No numeric columns available for ANOVA.")
        else:
            subject = st.selectbox("Choose the course/subject", options=subj_options)
            res = anova_and_eta_squared(merged, subject, 'Track')
            if res.get('f') is None and res.get('p') is None:
                st.warning(res.get('note', 'Cannot compute ANOVA.'))
            else:
                st.write(f"Variable: {subject}")
                st.write(f"F = {res['f']:.3f}" if res['f'] is not None else "F = N/A")
                st.write(f"p = {res['p']:.4f}" if res['p'] is not None else "p = N/A (scipy not available to compute exact p)")
                st.write(f"Eta-squared = {res['eta2']:.3f}" if res['eta2'] is not None else "Eta-squared = N/A")
                st.caption(f"Number of evaluated groups: {res.get('groups')}")

if st.checkbox("KMeans student profiles"):
    cols = [c for c in STANDARD_SCORE_COLS if c in merged.columns]
    n = st.slider("Clusters", 2, min(2, max(2, min(10, len(merged)))), 3)
    out = kmeans_profiles(merged, cols=cols, n_clusters=n)
    if out:
        clustered, centers = out
        st.dataframe(clustered.groupby('cluster')[cols].mean().round(2))
    else:
        st.info("Not enough data to cluster.")

if st.checkbox("PCA projection (2D)"):
    pca_res = pca_transform(merged)
    if pca_res:
        proj, var = pca_res
        fig = px.scatter(proj, x='PC1', y='PC2', color=merged.loc[proj.index,'Track'] if 'Track' in merged.columns else None)
        st.plotly_chart(fig)
        st.write("Explained variance:", var)
    else:
        st.info("Not enough numeric rows.")

# Simple logistic regression model for success prediction
st.subheader("Simple success prediction using sklearn")
if 'Passed' in merged.columns and LogisticRegression is not None:
    st.write("Train a simple model to estimate the probability of success according to grades")
    if st.button("Train model"):
        use_cols = [c for c in STANDARD_SCORE_COLS if c in merged.columns]
        df_model = merged.dropna(subset=use_cols+['Passed'])
        X = df_model[use_cols].fillna(df_model[use_cols].mean())
        y = df_model['Passed'].astype(int)
        if len(df_model) < 20:
            st.warning("Not enough labeled data to produce a reliable model (>=20 recommended)")
        scaler = StandardScaler()
        X = pd.DataFrame(X, columns=use_cols)
        Xs = scaler.fit_transform(X)
        Xtr, Xte, ytr, yte = train_test_split(Xs, y, test_size=0.2, random_state=42)
        model = LogisticRegression(max_iter=200)
        model.fit(Xtr, ytr)
        acc = model.score(Xte, yte)
        st.success(f"Model trained — accuracy (test): {acc:.2f}")
        st.session_state['simple_model'] = {'model': model, 'scaler': scaler, 'cols': use_cols}
    if st.session_state.get('simple_model'):
        inp = {}
        for c in st.session_state['simple_model']['cols']:
            inp[c] = st.number_input(f"{c}", value=float(merged[c].mean(skipna=True) if c in merged.columns else 0.0), key=f'inp_{c}')
        if st.button("Predict probability"):
            Xnew = np.array([inp[c] for c in st.session_state['simple_model']['cols']]).reshape(1,-1)
            Xs = st.session_state['simple_model']['scaler'].transform(Xnew)
            prob = st.session_state['simple_model']['model'].predict_proba(Xs)[0,1]
            st.write(f"Estimated probability of success: {prob*100:.1f}%")
else:
    if 'Passed' in merged.columns and LogisticRegression is None:
        st.info("scikit-learn not available — prediction is disabled")
    else:
        st.info("Missing 'Passed' column — prediction is disabled")

# ==================== EXPORTS ====================

st.header("Exports")
now = datetime.now().strftime('%Y%m%d_%H%M%S')

# CSV export
data_csv = merged.to_csv(index=False).encode('utf-8')
if st.session_state.authenticated:
    st.download_button(label="Download merged CSV",data=data_csv,file_name=f"merged_{now}.csv",mime="text/csv")

# Excel export (merged data + track summary)
out = io.BytesIO()
with pd.ExcelWriter(out, engine='xlsxwriter') as writer:
    merged.to_excel(writer, sheet_name='Merged', index=False)
    track_stats.to_excel(writer, sheet_name='Track Summary', index=False)

data_xlsx = out.getvalue()
if st.session_state.authenticated:
    st.download_button(label="Download Excel summary",data=data_xlsx,file_name=f"summary_{now}.xlsx",mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

# PDF export (boxplots per subject)
pdf_bytes = io.BytesIO()
with PdfPages(pdf_bytes) as pdf:
    for c in STANDARD_SCORE_COLS:
        if c in merged.columns:
            fig, ax = plt.subplots(figsize=(8,4))
            merged.boxplot(column=c, by='Track', ax=ax)
            ax.set_title(f'{c} by Track')
            plt.suptitle('')
            pdf.savefig(fig)
            plt.close(fig)

data_pdf = pdf_bytes.getvalue()
if st.session_state.authenticated:
    st.download_button(label="Download base PDF report (boxplot per subject)",data=data_pdf,file_name=f"report_{now}.pdf",mime='application/pdf')

# PDF export (whole page visuals)
def generate_comprehensive_pdf():
    """Generate PDF with all available visualizations: heatmap, distributions, comparisons, radar, outliers, PCA, etc."""
    pdf_bytes = io.BytesIO()
    with PdfPages(pdf_bytes) as pdf:
        # Page 1: Title & summary statistics
        fig = plt.figure(figsize=(8.5, 11))
        ax = fig.add_subplot(111)
        ax.axis('off')
        title_text = f"Comprehensive Data Report\n{datetime.now().strftime('%Y-%m-%d %H:%M')}"
        ax.text(0.5, 0.95, title_text, ha='center', fontsize=16, fontweight='bold', transform=ax.transAxes)
        summary_text = f"Total Students: {len(merged)}\nTracks: {merged['Track'].nunique()}\nQuality Score: {qs}/100"
        ax.text(0.5, 0.80, summary_text, ha='center', fontsize=11, transform=ax.transAxes, family='monospace')
        pdf.savefig(fig, bbox_inches='tight')
        plt.close(fig)

        # Page 2-N: Boxplots per subject by track
        for c in STANDARD_SCORE_COLS:
            if c in merged.columns:
                fig, ax = plt.subplots(figsize=(8,5))
                merged.boxplot(column=c, by='Track', ax=ax)
                ax.set_title(f'{c} Distribution by Track')
                plt.suptitle('')
                pdf.savefig(fig, bbox_inches='tight')
                plt.close(fig)

        # Correlation heatmap
        numeric_cols = [c for c in STANDARD_SCORE_COLS if c in merged.columns]
        if len(numeric_cols) >= 2:
            corr = merged[numeric_cols].corr()
            fig, ax = plt.subplots(figsize=(8,7))
            cax = ax.matshow(corr, cmap='viridis', aspect='auto', vmin=-1, vmax=1)
            fig.colorbar(cax)
            ax.set_xticks(range(len(numeric_cols)))
            ax.set_xticklabels(numeric_cols, rotation=45, ha='right')
            ax.set_yticks(range(len(numeric_cols)))
            ax.set_yticklabels(numeric_cols)
            ax.set_title('Subject Correlation Matrix')
            pdf.savefig(fig, bbox_inches='tight')
            plt.close(fig)

        # Histograms for each score column
        for c in numeric_cols:
            fig, ax = plt.subplots(figsize=(8,5))
            ax.hist(merged[c].dropna(), bins=20, color='skyblue', edgecolor='black')
            ax.set_xlabel(c)
            ax.set_ylabel('Frequency')
            ax.set_title(f'Distribution: {c}')
            pdf.savefig(fig, bbox_inches='tight')
            plt.close(fig)

        # Track comparison: side-by-side means
        tracks = list(merged['Track'].unique())
        if len(tracks) >= 2:
            fig, ax = plt.subplots(figsize=(10,6))
            track_means = {}
            for t in tracks:
                track_means[t] = [merged[merged['Track']==t][c].mean(skipna=True) for c in numeric_cols]
            x = np.arange(len(numeric_cols))
            width = 0.8 / len(tracks)
            for i, t in enumerate(tracks):
                ax.bar(x + i*width, track_means[t], width, label=t)
            ax.set_xlabel('Subjects')
            ax.set_ylabel('Average Score')
            ax.set_title('Track Comparison: Average Scores by Subject')
            ax.set_xticks(x + width * (len(tracks)-1) / 2)
            ax.set_xticklabels(numeric_cols, rotation=45, ha='right')
            ax.legend()
            pdf.savefig(fig, bbox_inches='tight')
            plt.close(fig)

        # Income vs No-Income comparison (if available)
        if 'IncomeStudent' in merged.columns:
            income_data = compute_income_comparison_stats(merged)
            if not income_data.empty:
                fig, ax = plt.subplots(figsize=(10,6))
                x = np.arange(len(income_data))
                width = 0.35
                ax.bar(x - width/2, income_data['Income Mean'], width, label='Income Support', color='orange')
                ax.bar(x + width/2, income_data['No-Income Mean'], width, label='No Income Support', color='green')
                ax.set_xlabel('Subjects')
                ax.set_ylabel('Average Score')
                ax.set_title('Performance: Income Support vs No Income Support')
                ax.set_xticks(x)
                ax.set_xticklabels(income_data['Subject'], rotation=45, ha='right')
                ax.legend()
                pdf.savefig(fig, bbox_inches='tight')
                plt.close(fig)

        # Outlier detection plot
        outmask = detect_outliers(merged)
        if outmask.sum() > 0:
            fig, ax = plt.subplots(figsize=(8,5))
            ax.scatter(range(len(merged)), [1]*len(merged), alpha=0.3, label='Normal', s=30)
            ax.scatter(np.where(outmask)[0], [1]*outmask.sum(), color='red', label='Outliers', s=100, marker='X')
            ax.set_ylabel('Status')
            ax.set_xlabel('Student Index')
            ax.set_title(f'Outlier Detection (Z-score): {int(outmask.sum())} outliers found')
            ax.set_yticks([])
            ax.legend()
            pdf.savefig(fig, bbox_inches='tight')
            plt.close(fig)

        # PCA projection (if enough data)
        pca_res = pca_transform(merged)
        if pca_res:
            proj, var = pca_res
            fig, ax = plt.subplots(figsize=(8,6))
            colors = pd.factorize(merged.loc[proj.index,'Track'] if 'Track' in merged.columns else 'All')[0]
            scatter = ax.scatter(proj['PC1'], proj['PC2'], c=colors, cmap='viridis', s=50, alpha=0.6)
            ax.set_xlabel(f'PC1 ({var[0]*100:.1f}%)')
            ax.set_ylabel(f'PC2 ({var[1]*100:.1f}%)')
            ax.set_title('PCA Projection (2D)')
            plt.colorbar(scatter, ax=ax, label='Track')
            pdf.savefig(fig, bbox_inches='tight')
            plt.close(fig)

        # Radar chart for each track
        if len(tracks) >= 1 and len(numeric_cols) >= 3:
            for track in tracks:
                df_track = merged[merged['Track']==track]
                vals = [df_track[c].mean(skipna=True) for c in numeric_cols]
                angles = np.linspace(0, 2*np.pi, len(numeric_cols), endpoint=False).tolist()
                vals += vals[:1]
                angles += angles[:1]
                fig = plt.figure(figsize=(6,6))
                ax = fig.add_subplot(111, polar=True)
                ax.plot(angles, vals, 'o-', linewidth=2, label=track)
                ax.fill(angles, vals, alpha=0.25)
                ax.set_thetagrids(np.degrees(angles[:-1]), numeric_cols)
                ax.set_ylim(0, max(vals[:-1])*1.1 if vals[:-1] else 100)
                ax.set_title(f'Profile: {track}')
                pdf.savefig(fig, bbox_inches='tight')
                plt.close(fig)

        # Summary statistics page
        fig = plt.figure(figsize=(8.5, 11))
        ax = fig.add_subplot(111)
        ax.axis('off')
        summary_stats = compute_basic_stats(merged)
        summary_text = "Summary Statistics:\n\n"
        for col, stats_dict in summary_stats.items():
            summary_text += f"{col}:\n"
            summary_text += f"  Mean: {stats_dict['mean']:.2f} | Median: {stats_dict['median']:.2f} | Std: {stats_dict['std']:.2f} | N: {stats_dict['count']}\n\n"
        ax.text(0.05, 0.95, summary_text, ha='left', va='top', fontsize=9, transform=ax.transAxes, family='monospace')
        pdf.savefig(fig, bbox_inches='tight')
        plt.close(fig)

    return pdf_bytes.getvalue()

# Download button for comprehensive PDF
full_pdf_data = generate_comprehensive_pdf()
if st.session_state.authenticated:
        st.download_button(label="Download Full PDF Report",data=full_pdf_data,file_name=f"full_report_{now}.pdf",mime='application/pdf')

# ZIP export (all files)
mem = io.BytesIO()
with zipfile.ZipFile(mem, mode='w') as zf:
    zf.writestr(f'merged_{now}.csv', merged.to_csv(index=False))
    zf.writestr(f'summary_{now}.xlsx', data_xlsx)

data_zip = mem.getvalue()
if st.session_state.authenticated:
    st.download_button(label="Download ZIP export",data=data_zip,file_name=f"full_export_{now}.zip",mime='application/zip')

# Session metadata export
with st.sidebar:
    st.header("Session state export")
    state = {'merged_rows': len(merged),'columns': list(merged.columns),'generated': now}
    data_json = json.dumps(state, indent=2).encode('utf-8')
    if st.session_state.authenticated:
        st.download_button(label="Download session JSON",data=data_json,file_name=f"session_{now}.json",mime='application/json')