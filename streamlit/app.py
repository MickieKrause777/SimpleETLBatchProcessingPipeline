import streamlit as st
import pandas as pd
from pymongo import MongoClient
import plotly.express as px
from datetime import datetime, timedelta
import os

st.set_page_config(page_title="IoT Sensor Dashboard", layout="wide")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017/")
MONGO_DB = os.getenv("MONGO_DB", "sensor_data")

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
collection = db["sensor_readings"]

st.title("IoT Sensor Dashboard")

st.sidebar.title("🔗 System Links")

st.sidebar.link_button("📈 Prometheus", "http://localhost:9090")
st.sidebar.link_button("📊 Grafana", "http://localhost:3000")
st.sidebar.link_button("⚙️ Airflow", "http://localhost:8080")
st.sidebar.link_button("🗄️ Mongo Express", "http://localhost:8081")

# ---- Device Selection ----

devices = collection.distinct("device")

device = st.sidebar.selectbox(
    "Select Device",
    devices
)

# ---- Time Range ----

def get_last_timestamp():
    doc = collection.find_one(sort=[("ts", -1)])
    if doc:
        return doc["ts"]
    return datetime.now()

end_time = get_last_timestamp()
start_time = end_time - timedelta(hours=24)

date_range = st.sidebar.date_input(
    "Date Range",
    [start_time, end_time]
)

start = datetime.combine(date_range[0], datetime.min.time())
end = datetime.combine(date_range[1], datetime.max.time())

# ---- Query Mongo ----

@st.cache_data(ttl=60)
def load_data(device, start, end):

    data = list(collection.find(
        {
            "device": device,
            "ts": {"$gte": start, "$lte": end}
        },
        {"_id": 0}
    ).sort("ts", 1))

    if not data:
        return pd.DataFrame()

    return pd.DataFrame(data)

df = load_data(device, start, end)

if df.empty:
    st.warning("No data found")
    st.stop()

# ---- KPIs ----

col1, col2, col3 = st.columns(3)

col1.metric("Avg Temp", f"{df['temp'].mean():.2f} °C")
col2.metric("Avg Humidity", f"{df['humidity'].mean():.2f} %")
col3.metric("Readings", len(df))

# ---- Sensor Charts ----

fig_temp = px.line(df, x="ts", y="temp", title="Temperature")
fig_humidity = px.line(df, x="ts", y="humidity", title="Humidity")
fig_co = px.line(df, x="ts", y="co", title="CO Level")

st.plotly_chart(fig_temp, use_container_width=True)
st.plotly_chart(fig_humidity, use_container_width=True)
st.plotly_chart(fig_co, use_container_width=True)