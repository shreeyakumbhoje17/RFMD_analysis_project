from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, List
import pandas as pd
from datetime import date
from google.cloud import bigquery
import os

# ---------- BigQuery Config ----------
BQ_PROJECT = "rfmdmarketing"       # <--- replace with your GCP project ID
BQ_DATASET = "rfmdAnalysis"
BQ_TABLE = "homeowners"

# Initialize BigQuery client
bq_client = bigquery.Client.from_service_account_json("/app/service-account.json")

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

# ---------- Load Data ----------
def load_base_df() -> pd.DataFrame:
    query = f"""
        SELECT *
        FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`
    """
    try:
        df = bq_client.query(query).to_dataframe()

        # Fix inconsistent column names
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

# Load once at startup
BASE_DF = load_base_df()

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
        df = BASE_DF.copy()
        df = df.where(pd.notnull(df), None)   # convert NaN → None
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# =============================================================
# 2) KPI CARD DATA (Row 1)
# =============================================================
@app.get("/homeowners/kpis")
def get_kpis():
    df = BASE_DF.copy()

    return {
        "total_customers": len(df),
        "avg_rfmd": round(df["RFMD_score"].mean(), 2),
        "avg_monetary": round(df["monetary"].mean(), 2),
        "avg_frequency": round(df["frequency"].mean(), 2),
        "top_trade": df["Trade"].value_counts().idxmax(),
        "top_region": df["region"].value_counts().idxmax()
    }

# =============================================================
# 3) TOP 10 (Row 2 - Left Table)
# =============================================================
@app.get("/homeowners/top10")
def get_top10():
    df = BASE_DF.copy()
    df_clean = df.drop(columns=["area"], errors="ignore")
    top10 = df_clean.sort_values("RFMD_score", ascending=False).head(10)
    return top10.to_dict(orient="records")

# =============================================================
# 4) RADAR CHART DATA (Row 2 - Right)
# =============================================================
@app.get("/homeowners/radar")
def get_radar(segment: str = None):
    """
    Returns aggregated R/F/M/D scores.
    If segment is provided → return that segment's averages.
    If not → return mean across all segments.
    """
    df = BASE_DF.copy()
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
# 5) REGION-FILTERED TRADE COUNTS (Row 3 - Right Chart)
# =============================================================
@app.get("/homeowners/tradecounts")
def get_tradecounts(region: str = None, sub_region: str = None):
    df = BASE_DF.copy()

    if region and region != "All Regions":
        df = df[df["region"] == region]

    if sub_region and sub_region != "All Sub-Regions":
        df = df[df["sub_region"] == sub_region]

    trade_counts = df["Trade"].value_counts().reset_index()
    trade_counts.columns = ["Trade", "Count"]

    return trade_counts.to_dict(orient="records")

# =============================================================
# 6) SUMMARY BOX (Row 3 - Left)
# =============================================================
@app.get("/homeowners/summary")
def get_summary():
    df = BASE_DF.copy()

    total_customers = len(df)
    avg_rfmd = round(df["RFMD_score"].mean(), 2)
    top_trade = df["Trade"].value_counts().idxmax()
    top_region = df["region"].value_counts().idxmax()
    avg_monetary = round(df["monetary"].mean(), 2)
    avg_frequency = round(df["frequency"].mean(), 2)

    # Best segment based on RFMD
    seg_scores = df.groupby("segment")["RFMD_score"].mean().sort_values(ascending=False)
    best_segment = seg_scores.index[0]
    best_segment_score = round(seg_scores.iloc[0], 2)

    # Highest revenue region
    reg_scores = df.groupby("region")["monetary"].sum().sort_values(ascending=False)
    best_region_rev = reg_scores.index[0]
    best_region_rev_val = round(reg_scores.iloc[0], 2)

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

