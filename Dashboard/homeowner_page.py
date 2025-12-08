import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import altair as alt

PRIMARY_COLOR = "#199e84"
SECONDARY_COLORS = ["#17becf", "#1f77b4", "#2ca02c", "#3fb3a3"]

# ================================
# ROW 1 — KPI NUMBERS
# ================================
def display_kpis(df):
    """Display KPI cards based on filtered df"""
    
    PRIMARY_COLOR = "#199e84"
    
    total_customers = len(df) if not df.empty else 0
    avg_monetary = round(df["monetary"].mean(), 2) if not df.empty else 0
    top_trade = df["Trade"].value_counts().idxmax() if not df.empty else "N/A"
    top_region = df["region"].value_counts().idxmax() if not df.empty else "N/A"

    # Highest and lowest revenue regions
    region_spending = df.groupby("region")["monetary"].sum() if not df.empty else pd.Series()
    highest_rev_region = region_spending.idxmax() if not region_spending.empty else "N/A"
    highest_rev_val = round(region_spending.max(), 2) if not region_spending.empty else 0
    lowest_rev_region = region_spending.idxmin() if not region_spending.empty else "N/A"
    lowest_rev_val = round(region_spending.min(), 2) if not region_spending.empty else 0

    kpi_labels = [
        "Total Customers",
        "Avg Monetary (£)",
        "Top Trade",
        "Top Region",
        "Highest Revenue Region",
        "Lowest Revenue Region"
    ]

    kpi_values = [
        total_customers,
        f"£{avg_monetary}",
        top_trade,
        top_region,
        f"{highest_rev_region} (£{highest_rev_val})",
        f"{lowest_rev_region} (£{lowest_rev_val})"
    ]

    col1, col2, col3, col4, col5, col6 = st.columns(6)
    cols = [col1, col2, col3, col4, col5, col6]

    for col, label, value in zip(cols, kpi_labels, kpi_values):
        card_html = f"""
        <div style="
            background-color: #f0f9f8;
            padding: 16px;
            border-radius: 14px;
            text-align:center;
            color:{PRIMARY_COLOR};
            font-family: Arial, sans-serif;
            height: 90px;
            box-shadow: 0px 2px 4px rgba(0,0,0,0.05);
        ">
            <p style='font-size:26px; font-weight:bold; margin:0;'>{value}</p>
            <p style='font-size:14px; margin:0; opacity:0.75;'>{label}</p>
        </div>
        """
        col.markdown(card_html, unsafe_allow_html=True)



# ================================
# ROW 2 — Top 10 Table + Radar Chart
# ================================
def display_row2(df):
    col1, col2 = st.columns([2, 0.5])

    # ----------- Top 10 Table -----------
    with col1:
        st.markdown('<h3 style="font-size:20px;">Top 10 Customers by RFMD Score</h3>',
                    unsafe_allow_html=True)

        df_clean = df.drop(columns=["area"], errors="ignore")
        top10 = df_clean.sort_values("RFMD_score", ascending=False).head(10)
        st.dataframe(top10, width='stretch', height=380)

    # ----------- Radar Chart -----------
    with col2:
        st.markdown('<h4 style="font-size:20px;">RFMD Radar (Filtered Segment)</h4>',
                    unsafe_allow_html=True)

        score_cols = ['R_score', 'F_score', 'M_score', 'D_score']
        segment_rfmd = df.groupby('segment')[score_cols].mean()

        if len(df["segment"].unique()) == 1:
            seg_data = segment_rfmd.loc[df["segment"].unique()[0]]
        else:
            seg_data = segment_rfmd.mean()

        labels = ['Recency', 'Frequency', 'Monetary', 'Duration']
        angles = np.linspace(0, 2 * np.pi, len(labels), endpoint=False)
        angles = np.concatenate((angles, [angles[0]]))

        values = seg_data.tolist() + [seg_data.tolist()[0]]

        fig, ax = plt.subplots(figsize=(2, 2), subplot_kw=dict(polar=True))
        ax.plot(angles, values, linewidth=2, color=PRIMARY_COLOR)
        ax.fill(angles, values, alpha=0.3, color=PRIMARY_COLOR)
        ax.set_xticks(angles[:-1])
        ax.set_xticklabels(labels, fontsize=7)
        ax.set_ylim(0, 5)

        st.pyplot(fig)
        plt.close(fig)

# ================================
# ROW 3 — Summary + Trade Distribution
# ================================
def display_row3(df_filtered):
    import streamlit as st
    import altair as alt

    PRIMARY_COLOR = "#199e84"
    COLOR_PALETTE = [
        PRIMARY_COLOR, "#17becf", "#1f77b4", "#2ca02c",
        "#3fb3a3", "#66c2ff", "#99e600"
    ]

    # ------------------------------
    # Region & Sub-Region Filters
    # ------------------------------
    st.markdown("### Region & Sub-Region Filters (affect Summary + Trade Chart)")
    region_list = ["All Regions"] + sorted(df_filtered["region"].fillna("Unknown").unique())
    selected_region = st.selectbox("Select Region:", region_list)

    if selected_region == "All Regions":
        available_subregions = sorted(df_filtered["sub_region"].unique())
    else:
        available_subregions = sorted(df_filtered[df_filtered["region"] == selected_region]["sub_region"].unique())

    selected_subregion = st.selectbox(
        "Select Sub-Region:",
        ["All Sub-Regions"] + available_subregions
    )

    # ------------------------------
    # Apply all filters: segment, region, sub-region
    # ------------------------------
    df_filtered_final = df_filtered.copy()
    if selected_region != "All Regions":
        df_filtered_final = df_filtered_final[df_filtered_final["region"] == selected_region]
    if selected_subregion != "All Sub-Regions":
        df_filtered_final = df_filtered_final[df_filtered_final["sub_region"] == selected_subregion]

    # ------------------------------
    # Columns layout
    # ------------------------------
    col1, col2 = st.columns([1, 3])

    # -------- Column 1: Summary Box --------
    with col1:
        total_customers = len(df_filtered_final) if not df_filtered_final.empty else 0
        total_rfmd = round(df_filtered_final["RFMD_score"].sum(), 2) if not df_filtered_final.empty else 0
        avg_frequency = round(df_filtered_final["frequency"].mean(), 2) if not df_filtered_final.empty else 0
        avg_spending = round(df_filtered_final["monetary"].mean(), 2) if not df_filtered_final.empty else 0
        top_trade = df_filtered_final["Trade"].value_counts().idxmax() if not df_filtered_final.empty else "N/A"

        # Display summary
        st.markdown("### Customer Insight Summary")
        st.write(f"**Total Customers:** {total_customers}")
        st.write(f"**Total RFMD:** {total_rfmd}")
        st.write(f"**Average Frequency:** {avg_frequency} visits")
        st.write(f"**Average Spending (Monetary):** £{avg_spending}")
        st.write(f"**Top Trade Category:** {top_trade}")

        # -------- Sub-Region Revenue KPIs (ignore sub-region filter) --------
        df_subregion_kpi = df_filtered.copy()
        if selected_region != "All Regions":
            df_subregion_kpi = df_subregion_kpi[df_subregion_kpi["region"] == selected_region]

        if not df_subregion_kpi.empty:
            subregion_revenue = df_subregion_kpi.groupby("sub_region")["monetary"].sum()

            highest_subregion = subregion_revenue.idxmax() if not subregion_revenue.empty else "N/A"
            highest_subregion_val = round(subregion_revenue.max(), 2) if not subregion_revenue.empty else 0

            lowest_subregion = subregion_revenue.idxmin() if not subregion_revenue.empty else "N/A"
            lowest_subregion_val = round(subregion_revenue.min(), 2) if not subregion_revenue.empty else 0

            st.markdown("### Sub-Region Revenue Insights")
            st.write(f"**Highest Revenue Sub-Region:** {highest_subregion} (£{highest_subregion_val})")
            st.write(f"**Lowest Revenue Sub-Region:** {lowest_subregion} (£{lowest_subregion_val})")
        else:
            st.info("No data available for sub-region revenue insights.")

    # -------- Column 2: Trade chart --------
    with col2:
        trade_counts = df_filtered_final["Trade"].value_counts().reset_index()
        trade_counts.columns = ["Trade", "Count"]
        st.markdown("### Trade Distribution (Region-filtered)")
        if not trade_counts.empty:
            chart_trade = (
                alt.Chart(trade_counts)
                .mark_bar()
                .encode(
                    y=alt.Y("Trade:N", sort="-x"),
                    x=alt.X("Count:Q"),
                    color=alt.Color("Trade:N", scale=alt.Scale(range=COLOR_PALETTE)),
                    tooltip=["Trade:N", "Count:Q"]
                )
                .properties(width=450, height=320)
            )
            st.altair_chart(chart_trade, width='stretch')
        else:
            st.info("No trade data available for selected region.")







# ================================
# MAIN PAGE ENTRY
# ================================
def display_option1(df, api_url):
    """Segment filter applied here, passed to all charts"""
    segments = df["segment"].unique().tolist()
    selected_segment = st.sidebar.selectbox("Select Segment", ["All"] + segments)

    if selected_segment != "All":
        filtered_df = df[df["segment"] == selected_segment]
    else:
        filtered_df = df.copy()

    # Pass filtered df to all sections
    display_kpis(filtered_df)
    display_row2(filtered_df)
    display_row3(filtered_df)


        