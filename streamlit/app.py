import tempfile
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.pagesizes import A4, landscape
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

@st.cache_data(ttl=60)
def load_anomalies(start, end):
    alerts_data = list(db['sensor_alerts'].find({
        'ts': {'$gte': start, '$lte': end}
    }))
    print(alerts_data)
    if not alerts_data:
        return pd.DataFrame()

    return pd.DataFrame(alerts_data)

alerts_df = load_anomalies(start, end)

if alerts_df.empty:
    st.warning("No Anomalies found.")

# --- Alerts ---
def create_anomaly_report():
    if alerts_df.empty:
        return None

    report_df = alerts_df.groupby('device').agg({
        'anomaly_count': 'sum',
        'anomalies': lambda x: [item for sublist in x for item in sublist],
        'sensor_malfunction': 'sum',
        'hvac_waste': 'sum',
        'ventilation_ineff': 'sum'
    }).reset_index()

    pdf_path = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf").name
    doc = SimpleDocTemplate(pdf_path, pagesize=landscape(A4))
    styles = getSampleStyleSheet()
    story = []

    story.append(Paragraph(f"Sensor Alerts Report ({datetime.date(start)} - {datetime.date(end)})", styles['Title']))
    story.append(Spacer(1, 20))
    story.append(Spacer(1, 20))

    for _, row in report_df.iterrows():
        device = row["device"]

        story.append(Paragraph(f"Device {row['device']} - Total Anomalies: {row['anomaly_count']}", styles['Heading3']))

        story.append(Paragraph(f"Types: {', '.join(set(row['anomalies']))}", styles['Normal']))

        recent = alerts_df[alerts_df["device"] == device].sort_values("ts").tail(3)

        active = []
        if not recent.empty:
            latest = recent.iloc[0]

            if latest["sensor_malfunction"]:
                active.append("Sensor Malfunction")

            if latest["hvac_waste"]:
                active.append("HVAC Waste")

            if latest["ventilation_ineff"]:
                active.append("Ventilation Inefficiency")

        for _, r in recent.iterrows():
            story.append(Paragraph(
                f"{r['ts']} → {', '.join(r['anomalies'])} | {', '.join(active) if active else ''}",
                styles['Normal']
            ))

        story.append(Paragraph(f"Please watch the Chart for further Information!"))
        story.append(Spacer(1, 10))

    doc.build(story)
    return pdf_path

anomaly_path = create_anomaly_report()

if anomaly_path:
    with open(anomaly_path, "rb") as f:
        st.download_button("Download Anomaly Report", f, file_name="Anomaly Report.pdf")


# ---- KPIs ----

col1, col2, col3 = st.columns(3)

col1.metric("Avg Temp", f"{df['temp'].mean():.2f} °C")
col2.metric("Avg Humidity", f"{df['humidity'].mean():.2f} %")
col3.metric("Readings", len(df))

# ---- Sensor Charts ----

fig_temp = px.line(df, y="temp", title="Temperature")
fig_humidity = px.line(df, y="humidity", title="Humidity")
fig_co = px.line(df, y="co", title="CO Level")

fig_temp.update_yaxes(range=[0, df["temp"].max()])
fig_humidity.update_yaxes(range=[0, df["humidity"].max()])
fig_co.update_yaxes(range=[0, df["co"].max()])

st.plotly_chart(fig_temp, use_container_width=True)
st.plotly_chart(fig_humidity, use_container_width=True)
st.plotly_chart(fig_co, use_container_width=True)


# ---- Generate PDF via Button for Device----
temp_img = tempfile.NamedTemporaryFile(suffix=".png", delete=False).name
hum_img  = tempfile.NamedTemporaryFile(suffix=".png", delete=False).name
co_img   = tempfile.NamedTemporaryFile(suffix=".png", delete=False).name

fig_temp.write_image(temp_img)
fig_humidity.write_image(hum_img)
fig_co.write_image(co_img)

def create_pdf():
    pdf_path = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf").name
    doc = SimpleDocTemplate(pdf_path, pagesize=landscape(A4))
    styles = getSampleStyleSheet()
    story = []

    story.append(Paragraph("Sensor Monitoring Report", styles["Title"]))
    story.append(Spacer(1, 20))

    story.append(Paragraph("Temperature", styles["Heading2"]))
    story.append(Image(temp_img, width=800, height=400))
    story.append(Spacer(1, 20))

    story.append(Paragraph("Humidity", styles["Heading2"]))
    story.append(Image(hum_img, width=800, height=400))
    story.append(Spacer(1, 20))

    story.append(Paragraph("CO Levels", styles["Heading2"]))
    story.append(Image(co_img, width=800, height=400))
    story.append(Spacer(1, 20))

    doc.build(story)
    return pdf_path

pdf_path = create_pdf()
with open(pdf_path, "rb") as f:
    st.download_button("Download Report as PDF", f, file_name=f"sensor_report_{device}_{datetime.date(start_time)}_{datetime.date(end_time)}.pdf")

