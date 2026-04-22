import streamlit as st
import requests
import pandas as pd

API = "https://healthcare-api-aygp.onrender.com"

st.set_page_config(page_title="Healthcare Dashboard", layout="wide")
st.title("🏥 Healthcare Analytics Dashboard")

# -----------------------
# Helper (with caching + basic error handling)
# -----------------------
@st.cache_data(ttl=60)
def fetch(endpoint, params=None):
    try:
        r = requests.get(f"{API}{endpoint}", params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"API error on {endpoint}: {e}")
        return None


# -----------------------
# Sidebar Filters (clean dropdowns)
# -----------------------
st.sidebar.header("🔍 Filters")

doctor = st.sidebar.selectbox("Doctor", ["All", "D001", "D002", "D003"])
dept = st.sidebar.selectbox("Department", ["All", "Pediatrics", "Oncology", "Dermatology"])

date_range = st.sidebar.date_input("Date Range", [])

params = {}
if doctor != "All":
    params["doctor"] = doctor
if dept != "All":
    params["dept"] = dept
if len(date_range) == 2:
    params["start_date"] = str(date_range[0])
    params["end_date"] = str(date_range[1])


# -----------------------
# Fetch Data
# -----------------------
with st.spinner("Loading data..."):
    rev = fetch("/revenue", params)
    top_docs = fetch("/top-doctors", params)
    dept_data = fetch("/department-load", params)
    trend = fetch("/trend")

# Convert safely
df_docs = pd.DataFrame(top_docs) if top_docs else pd.DataFrame()
df_dept = pd.DataFrame(dept_data) if dept_data else pd.DataFrame()

# Trend dataframe
df_trend = pd.DataFrame(trend) if trend else pd.DataFrame()
if not df_trend.empty:
    df_trend["appointment_date"] = pd.to_datetime(df_trend["appointment_date"])


# -----------------------
# 🔧 1. KPI Cards
# -----------------------
col1, col2, col3, col4 = st.columns(4)

with col1:
    if rev:
        st.metric("💰 Revenue", f"{rev['total_revenue']:,}")

with col2:
    st.metric("👨‍⚕️ Doctors", len(df_docs))

with col3:
    st.metric("🏥 Departments", df_dept["specialization"].nunique() if not df_dept.empty else 0)

with col4:
    st.metric("👥 Patients", len(df_docs))  # proxy metric


st.divider()


# -----------------------
# 🔧 4. Improved Layout
# -----------------------
st.subheader("📊 Insights Overview")

col1, col2 = st.columns(2)

# Top Doctors
with col1:
    st.subheader("👨‍⚕️ Top Doctors")
    if not df_docs.empty:
        st.bar_chart(df_docs.set_index("doctor_first_name")["revenue"])
    else:
        st.info("No data available")

# Department Load
with col2:
    st.subheader("🏥 Department Load")
    if not df_dept.empty:
        st.bar_chart(df_dept.set_index("specialization")["count"])
    else:
        st.info("No data available")


st.divider()


# -----------------------
# 🔧 5. Revenue Trend
# -----------------------
st.subheader("📈 Revenue Trend")

if not df_trend.empty:
    st.line_chart(df_trend.set_index("appointment_date")["revenue"])
else:
    st.info("No trend data available")


# -----------------------
# 🔧 6. Department Distribution (Bar instead of Pie)
# -----------------------
st.subheader("🥧 Department Distribution")

if not df_dept.empty:
    st.bar_chart(df_dept.set_index("specialization")["count"])
else:
    st.info("No department data available")