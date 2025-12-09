from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, List
import pandas as pd
from datetime import date
from google.cloud import bigquery
import os
import time

# ---------- BigQuery Config ----------
BQ_PROJECT = "rfmdmarketing"       
BQ_DATASET = "rfmdAnalysis"
BQ_TABLE = "homeowners"

# ---------- Service Account Path ----------
SERVICE_ACCOUNT_PATH = "/app/service-account.json"

# ---------- BigQuery Client ----------
bq_client = bigquery.Client.from_service_account_json(
    SERVICE_ACCOUNT_PATH,
    project=BQ_PROJECT
)

# ---------- Cache Settings ----------
CACHE = {"data": None, "last_refresh": 0}
REFRESH_INTERVAL = 15 * 24 * 60 * 60  # 15 days in seconds

def load_base_df() -> pd.DataFrame:
    """Query BigQuery and return dataframe."""
    query = f"""
        SELECT *
        FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`
    """
    try:
        df = bq_client.query(query).to_dataframe()
        df.columns = [c.replace(" ", "_") for c in df.columns]

        # Parse dates
        if "first_transaction" in df.columns:
            df["first_transaction"] = pd.to_datetime(
                df["first_transaction"], errors="coerce"
            ).dt.date

        if "last_transaction" in df.columns:
            df["last_transaction"] = pd.to_datetime(
                df["last_transaction"], errors="coerce"
            ).dt.date

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
app = FastAPI()

@app.get("/")
def root():
    return {"message": "RFMD API is running"}

# =============================================================
# 1) GET ALL HOMEOWNERS
# =============================================================
@app.get("/homeowners", response_model=List[Homeowner])
def get_homeowners():
    try:
        df = get_cached_data()
        df = df.where(pd.notnull(df), None)  # convert NaN → None
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# =============================================================
# 2) KPI CARD DATA
# =============================================================
@app.get("/homeowners/kpis")
def get_kpis():
    df = get_cached_data()
    return {
        "total_customers": len(df),
        "avg_rfmd": round(df["RFMD_score"].mean(), 2),
        "avg_monetary": round(df["monetary"].mean(), 2),
        "avg_frequency": round(df["frequency"].mean(), 2),
        "top_trade": df["Trade"].value_counts().idxmax() if not df.empty else "N/A",
        "top_region": df["region"].value_counts().idxmax() if not df.empty else "N/A"
    }

# =============================================================
# 3) TOP 10 CUSTOMERS
# =============================================================
@app.get("/homeowners/top10")
def get_top10():
    df = get_cached_data()
    df_clean = df.drop(columns=["area"], errors="ignore")
    top10 = df_clean.sort_values("RFMD_score", ascending=False).head(10)
    return top10.to_dict(orient="records")

# =============================================================
# 4) RADAR CHART DATA
# =============================================================
@app.get("/homeowners/radar")
def get_radar(segment: str = None):
    df = get_cached_data()
    grouped = df.groupby("segment")[["R_score", "F_score", "M_score", "D_score"]].mean()

    if segment and segment in grouped.index:
        seg_data = grouped.loc[segment].to_dict()
    else:
        seg_data = grouped.mean().to_dict()

    return {
        "labels": ["Recency", "Frequency", "Monetary", "Duration"],
        "scores": [
            seg_data["R_score"],
            seg_data["F_score"],
            seg_data["M_score"],
            seg_data["D_score"]
        ]
    }

# =============================================================
# 5) REGION-FILTERED TRADE COUNTS
# =============================================================
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

# =============================================================
# 6) SUMMARY BOX
# =============================================================
@app.get("/homeowners/summary")
def get_summary():
    df = get_cached_data()

    total_customers = len(df)
    avg_rfmd = round(df["RFMD_score"].mean(), 2)
    top_trade = df["Trade"].value_counts().idxmax() if not df.empty else "N/A"
    top_region = df["region"].value_counts().idxmax() if not df.empty else "N/A"
    avg_monetary = round(df["monetary"].mean(), 2)
    avg_frequency = round(df["frequency"].mean(), 2)

    # Best segment based on RFMD
    seg_scores = df.groupby("segment")["RFMD_score"].mean().sort_values(ascending=False)
    best_segment = seg_scores.index[0] if not seg_scores.empty else "N/A"
    best_segment_score = round(seg_scores.iloc[0], 2) if not seg_scores.empty else 0

    # Highest revenue region
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
