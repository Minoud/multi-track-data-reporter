# Multi-Track Data Reporter & Statistical Analyzer

![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python) ![Streamlit](https://img.shields.io/badge/Streamlit-1.30-orange?logo=streamlit) ![Pandas](https://img.shields.io/badge/Pandas-1.6.2-lightgrey?logo=pandas)  

---

## Overview

The Multi-Track Data Reporter is a Python-based tool designed to load, clean, merge, analyze, and visualize student performance data across multiple academic tracks (Finance, Data Science, Engineering, etc.).  

This app is interactive, powered by Streamlit, and generates statistical summaries, visualizations, and downloadable reports in CSV, Excel, PDF, and ZIP formats. It’s robust, reusable, and works with new Excel files from future years without manual code edits.  

---

## Quick Start
1. Clone the repo  
2. Install dependencies  
3. Create `.streamlit/secrets.toml`  
4. Run: `streamlit run code.py`  
5. Upload your Excel/CSV files in the sidebar

---

## Features

### 1. Data Loading & Cleaning
- Import all sheets from Excel files or CSV files dynamically.  
- Clean common inconsistencies: missing values (`NaN`, empty strings), waivers (`"Waived"`, `"N/A"`), and inconsistent formatting.  
- Normalize numeric fields: scores, attendance, project grades.  
- Automatically add a Track column to identify each sheet.  

### 2. Track-Level Statistics
- Count total students per track.  
- Compute average, median, and standard deviation for each subject.  
- Calculate average attendance and project scores.  
- Determine pass rates based on the `Passed` column.

### 3. Cohort & Income Analysis
- Analyze academic performance by cohort.  
- Compare income-supported students against others.  
- Highlight anomalies or outliers in subjects automatically.

### 4. Visual Insights
- Histograms, boxplots, and correlation matrices.  
- Radar charts for track-specific average profiles.  
- Interactive visualizations using Plotly and Matplotlib.

### 5. Duplicate Detection
- Detect duplicates based on selected column (typically `StudentID` or `StudentName`).  
- Remove duplicates with a single click in the UI.

### 6. Predictive Insights (Optional)
- Train a simple logistic regression model to estimate the probability of passing based on grades.  
- Input hypothetical student scores to predict their success probability.

### 7. Export Options
- **CSV**: merged dataset.  
- **Excel**: summary statistics, track-level reports.  
- **PDF**: charts and boxplots for each subject.  
- **ZIP**: bundle of all exported files.  
- **JSON**: session metadata for reproducibility.

---

## Installation

1. Clone the repository or download the project files:

```bash
git clone https://github.com/Minoud/multi-track-data-reporter
cd multi-track-data-reporter
```

2. Install required dependencies:

```bash
pip install pandas numpy matplotlib seaborn plotly streamlit scikit-learn openpyxl xlsxwriter python-pptx requests requests-oauthlib
```

> Optional: install `scipy` if you need advanced statistical tests.

3. Authentication:

To export reports, a Google authentication is set up.
<br>**Whether the authentication succeeds or not, you will be considered authenticated, so this part will not block you if you don't want to set up the API**
<br>
The authentication relies on a client_id and client_secret stored in <br>`multi-track-data-reporter/.streamlit/secrets.toml`
<br>
To execute this code locally, create this file and use this template:
```
oauth_redirect_uri = "http://localhost:8501/"

[oauth.google]
client_id = "client_id_mock_001.apps.googleusercontent.com"
client_secret = "client_secret_mock_ABCDEF123456"
```
If you wish to test a real version, you will need to create your own Google Cloud app and set up your own API and keys.


## Usage

1. Run the Streamlit app:

```bash
streamlit run code.py
```

2. Use the sidebar to upload Excel or CSV files.  
3. Preview and optionally manually map columns.  
4. Explore merged data, duplicates, and track-level statistics.  
5. Generate visualizations (distribution, radar charts, correlation matrix).  
6. Authenticate (optional password) to download reports in multiple formats.

---

## File Structure

```
multi-track-data-reporter/
│
├─ code.py       # Main Streamlit app
├─ oauth_state_store.json    # JSON file to save Streamlit state after authentification
└─ README.md                 # This documentation
```

---

## Assumptions

- Input Excel files maintain consistent column structure across sheets.  
- `Passed` column indicates success/failure (`Y/N`).  
- Attendance may be given as a fraction (0–1) or a percentage (0–100).  
- Missing or waived entries are automatically cleaned.

---

## Limitations

- PDF generation can be slow for very large datasets (>50k rows).  
- Logistic regression prediction requires ≥20 labeled rows for meaningful results.  
- Authentication is simple; not recommended for production environments.  

---

## Bonus Features

- Interactive duplicate detection and removal.  
- Trend analysis and outlier detection per track.  
- Full export package including CSV, Excel, PDF, ZIP, and JSON.  

---

## Future Enhancements

- Dashboard-style Excel outputs with advanced formatting.   
- Automated alerts for low-performing tracks or cohorts.  
- More advanced predictive models (random forest, gradient boosting).  

---

## Contact

- Maintainer: Minoud 
- GitHub Repository: `https://github.com/Minoud/multi-track-data-reporter`  


