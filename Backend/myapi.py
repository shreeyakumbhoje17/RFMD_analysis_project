from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional, List
import pandas as pd
from datetime import date
from google.cloud import bigquery
import os
import time

# ---------- BigQuery Config ----------
BQ_PROJECT = os.environ.get("BQ_PROJECT", "rfmdmarketing")
BQ_DATASET = os.environ.get("BQ_DATASET", "rfmdAnalysis")
BQ_TABLE = os.environ.get("BQ_TABLE", "homeowners")

# ---------- Service Account Path ----------
# Cloud Run will mount the secret here
SERVICE_ACCOUNT_PATH = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
if not SERVICE_ACCOUNT_PATH:
    raise RuntimeError(
        "GOOGLE_APPLICATION_CREDENTIALS environment variable not set. "
        "Ensure the service account JSON is mounted and the env variable is configured."
    )

# ---------- Lazy BigQuery Client ----------
def get_bq_client():
    return bigquery.Client.from_service_account_json(
        SERVICE_ACCOUNT_PATH,
        project=BQ_PROJECT
    )

# ---------- Cache Settings ----------
CACHE = {"data": None, "last_refresh": 0}
REFRESH_INTERVAL = 15 * 24 * 60 * 60  # 15 days

def load_base_df() -> pd.DataFrame:
    """Query BigQuery and return dataframe."""
    query = f"SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`"
    try:
        df = get_bq_client().query(query).to_dataframe()
        df.columns = [c.replace(" ", "_") for c in df.columns]

        # Parse dates safely
        for col in ["first_transaction", "last_transaction"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

        return df
    except Exception as e:
        print(f"Error loading data from BigQuery: {e}")
        return pd.DataFrame()

def get_cached_data():
    """Return cached dataset, refresh if interval exceeded."""
    now = time.time()
    if CACHE["data"] is None or now - CACHE["last_refresh"] > REFRESH_INTERVAL:
        CACHE["data"] = load_base_df()
        CACHE["last_refresh"] = now
    return CACHE["data"]

# ---------- Pydantic Model ----------
class Homeowner(BaseModel):
    customer_id: str
    first_transaction: Optional[date]
    last_transaction: Optional[date]
    frequency: Optional[int]
    monetary: Optional[float]
    recency: Optional[int]
    duration: Optional[int]
    segment: Optional[str]
    R_score: Optional[int]
    F_score: Optional[int]
    M_score: Optional[int]
    D_score: Optional[int]
    RFMD_score: Optional[float]
    cluster: Optional[int]
    Trade: Optional[str]
    Post_code: Optional[str] = None
    sub_region: Optional[str]
    region: Optional[str]

# ---------- FastAPI App ----------
app = FastAPI(title="RFMD API")

@app.get("/")
def root():
    return {"message": "RFMD API is running"}

@app.get("/homeowners", response_model=List[Homeowner])
def get_homeowners():
    df = get_cached_data()
    df = df.where(pd.notnull(df), None)  # NaN → None
    return df.to_dict(orient="records")

@app.get("/homeowners/kpis")
def get_kpis():
    df = get_cached_data()
    if df.empty:
        return {}
    return {
        "total_customers": len(df),
        "avg_rfmd": round(df["RFMD_score"].mean(), 2),
        "avg_monetary": round(df["monetary"].mean(), 2),
        "avg_frequency": round(df["frequency"].mean(), 2),
        "top_trade": df["Trade"].value_counts().idxmax() if not df.empty else "N/A",
        "top_region": df["region"].value_counts().idxmax() if not df.empty else "N/A"
    }

@app.get("/homeowners/top10")
def get_top10():
    df = get_cached_data()
    df_clean = df.drop(columns=["area"], errors="ignore")
    top10 = df_clean.sort_values("RFMD_score", ascending=False).head(10)
    return top10.to_dict(orient="records")

@app.get("/homeowners/radar")
def get_radar(segment: str = None):
    df = get_cached_data()
    grouped = df.groupby("segment")[["R_score","F_score","M_score","D_score"]].mean()
    if segment and segment in grouped.index:
        seg_data = grouped.loc[segment].to_dict()
    else:
        seg_data = grouped.mean().to_dict()
    return {
        "labels": ["Recency","Frequency","Monetary","Duration"],
        "scores": [
            seg_data.get("R_score", 0),
            seg_data.get("F_score", 0),
            seg_data.get("M_score", 0),
            seg_data.get("D_score", 0)
        ]
    }

@app.get("/homeowners/tradecounts")
def get_tradecounts(region: str = None, sub_region: str = None):
    df = get_cached_data()
    if region and region != "All Regions":
        df = df[df["region"] == region]
    if sub_region and sub_region != "All Sub-Regions":
        df = df[df["sub_region"] == sub_region]
    trade_counts = df["Trade"].value_counts().reset_index()
    trade_counts.columns = ["Trade", "Count"]
    return trade_counts.to_dict(orient="records")

@app.get("/homeowners/summary")
def get_summary():
    df = get_cached_data()
    if df.empty:
        return {}
    total_customers = len(df)
    avg_rfmd = round(df["RFMD_score"].mean(), 2)
    top_trade = df["Trade"].value_counts().idxmax() if not df.empty else "N/A"
    top_region = df["region"].value_counts().idxmax() if not df.empty else "N/A"
    avg_monetary = round(df["monetary"].mean(), 2)
    avg_frequency = round(df["frequency"].mean(), 2)
    seg_scores = df.groupby("segment")["RFMD_score"].mean().sort_values(ascending=False)
    best_segment = seg_scores.index[0] if not seg_scores.empty else "N/A"
    best_segment_score = round(seg_scores.iloc[0], 2) if not seg_scores.empty else 0
    reg_scores = df.groupby("region")["monetary"].sum().sort_values(ascending=False)
    best_region_rev = reg_scores.index[0] if not reg_scores.empty else "N/A"
    best_region_rev_val = round(reg_scores.iloc[0], 2) if not reg_scores.empty else 0
    return {
        "total_customers": total_customers,
        "avg_rfmd": avg_rfmd,
        "top_trade": top_trade,
        "top_region": top_region,
        "avg_monetary": avg_monetary,
        "avg_frequency": avg_frequency,
        "best_segment": best_segment,
        "best_segment_score": best_segment_score,
        "best_region_revenue": best_region_rev,
        "best_region_revenue_value": best_region_rev_val
    }
