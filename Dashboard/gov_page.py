import streamlit as st
import pandas as pd

def display_government(df: pd.DataFrame) -> None:
    st.title("Government RFMD Analysis")

    st.write("Overview of RFMD metrics for Government customers.")

    st.subheader("Raw RFMD Data (Government Segment)")
    if "segment" in df.columns:
        government_df = df[df["segment"] == "Government"]
    else:
        government_df = df

    st.dataframe(government_df)
