import streamlit as st
import pandas as pd

def display_corporate(df: pd.DataFrame) -> None:
    st.title("Corporate RFMD Analysis")

    st.write("Overview of RFMD metrics for Corporate customers.")

    st.subheader("Raw RFMD Data (Corporate Segment)")
    if "segment" in df.columns:
        corporate_df = df[df["segment"] == "Corporate"]
    else:
        corporate_df = df

    st.dataframe(corporate_df)