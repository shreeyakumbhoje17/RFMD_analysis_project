import streamlit as st
import pandas as pd
import requests
import os

# ------------------------------------------------
# Base API URL (from environment variable if set)
# ------------------------------------------------
API_URL = os.environ.get("API_URL", "http://backend:8000")



# ------------------------------------------------
# Streamlit Page Configuration
# ------------------------------------------------
st.set_page_config(
    page_title="Aspect RFMD Analysis Dashboard",
    layout="wide"
)

# Global CSS
st.markdown("""
<style>
    .stApp { padding-top: 1rem; }
    .block-container { padding: 1rem 2rem !important; }
</style>
""", unsafe_allow_html=True)

# ------------------------------------------------
# Load Data From FastAPI
# ------------------------------------------------
@st.cache_data
def load_data_from_api():
    url = f"{API_URL}/homeowners"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return pd.DataFrame(data)
    except Exception as e:
        st.error(f"Error connecting to API: {e}")
        return pd.DataFrame()

# ------------------------------------------------
# Title
# ------------------------------------------------
st.markdown(
    '<h1 style="font-size:3em; text-align:center;">Aspect RFMD Dashboard</h1>',
    unsafe_allow_html=True
)

# ------------------------------------------------
# Main Navigation
# ------------------------------------------------
import homeowner_page
import corporate_page
import insurance_page
import gov_page

def main():
    df = load_data_from_api()

    st.sidebar.title("Navigation")
    selected_option = st.sidebar.radio(
        "Go to page:",
        ("HomeOwner", "Corporate", "Insurance", "Government")
    )

    if df.empty:
        st.warning("No data loaded. Check API.")
        return

    if selected_option == "HomeOwner":
        homeowner_page.display_option1(df, API_URL)
    elif selected_option == "Corporate":
        corporate_page.display_option2(df)
    elif selected_option == "Insurance":
        insurance_page.display_option3(df)
    elif selected_option == "Government":
        gov_page.display_option4(df)

if __name__ == "__main__":
    main()
