import streamlit as st
import pandas as pd

def display_insurance(df: pd.DataFrame) -> None:
    st.title("Insurance RFMD Analysis")

    st.write("Overview of RFMD metrics for Insurance customers.")

    # Example basic view – adjust to your data model
    st.subheader("Raw RFMD Data (Insurance Segment)")
    # If you have a 'segment' column you can filter like this:
    if "segment" in df.columns:
        insurance_df = df[df["segment"] == "Insurance"]
    else:
        insurance_df = df  # fallback if no segment column yet

    st.dataframe(insurance_df)
