import streamlit as st
import pandas as pd
import json
import plotly.express as px
import plotly.graph_objects as go

# --- CONFIGURATION ---
st.set_page_config(page_title="Montreal Real Estate Inference", layout="wide")
DATA_PATH = "dashboard_inference_insights.json"

@st.cache_data
def load_data():
    with open(DATA_PATH, "r") as f:
        return json.load(f)

def clean_feature_name(name):
    """Cleans statsmodels categorical variable names for the UI."""
    return name.replace("C(inferred_region)[T.", "").replace("C(category)[T.", "").replace("]", "")

# --- LOAD DATA ---
try:
    data = load_data()
except FileNotFoundError:
    st.error(f"Data file {DATA_PATH} not found. Please run the inference pipeline first.")
    st.stop()

# --- HEADER & METRICS ---
st.title("Montreal Real Estate: Inference & Drift Analysis")
st.markdown("Analyzing structural market changes and feature importance shifts between the previous and current year.")

st.header("Global Model Performance")
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        label="Dataset Size (2026)", 
        value=f"{data['metadata']['rows_2026']:,}", 
        delta=f"{data['metadata']['rows_2026'] - data['metadata']['rows_2025']:,} rows"
    )
with col2:
    st.metric(
        label="Model R² (2026)", 
        value=f"{data['performance']['2026']['r2']:.3f}", 
        delta=f"{data['performance']['2026']['r2'] - data['performance']['2025']['r2']:.3f}"
    )
with col3:
    st.metric(
        label="Mean Absolute Error (2026)", 
        value=f"${data['performance']['2026']['mae']:,.0f}", 
        delta=f"${data['performance']['2026']['mae'] - data['performance']['2025']['mae']:,.0f}",
        delta_color="inverse"
    )

st.divider()

# --- TABS ---
tab1, tab2, tab3 = st.tabs(["SHAP Feature Importance", "Structural Market Drift", "Regional Price Premiums"])

# --- TAB 1: SHAP ---
with tab1:
    st.subheader("Shifts in Pricing Drivers (SHAP Value Drift)")
    
    df_shap = pd.DataFrame(data['shap_importance_drift'])
    df_shap_melted = df_shap.melt(id_vars=["feature", "delta"], 
                                  value_vars=["importance_2025", "importance_2026"],
                                  var_name="Year", value_name="Mean Absolute SHAP (Log Price)")
    df_shap_melted["Year"] = df_shap_melted["Year"].str.replace("importance_", "")

    fig_shap = px.bar(
        df_shap_melted, 
        x="feature", 
        y="Mean Absolute SHAP (Log Price)", 
        color="Year", 
        barmode="group",
        color_discrete_sequence=["#3498db", "#fa8c05"],
        title="Overall Feature Impact on Log Price Predictions"
    )
    st.plotly_chart(fig_shap, use_container_width=True)

# --- TAB 2: STRUCTURAL DRIFT ---
with tab2:
    st.subheader("Statistically Significant Coefficient Shifts (p < 0.05)")
    st.markdown("Features where the market valuation changed significantly from the previous year. Calculated via Z-test on GLM coefficients.")
    
    df_drift = pd.DataFrame(data['coefficient_drift'])
    df_drift["feature_clean"] = df_drift["feature"].apply(clean_feature_name)
    
    # Filter for significance and sort by magnitude of change
    df_sig_drift = df_drift[df_drift["significant"] == True].copy()
    df_sig_drift["abs_delta"] = df_sig_drift["delta"].abs()
    df_sig_drift = df_sig_drift.sort_values(by="abs_delta", ascending=False).drop(columns=["abs_delta"])

    st.dataframe(
        df_sig_drift[["feature_clean", "coef_2025", "coef_2026", "delta", "p_value"]].style.format({
            "coef_2025": "{:.4f}",
            "coef_2026": "{:.4f}",
            "delta": "{:.4f}",
            "p_value": "{:.4f}"
        }),
        use_container_width=True,
        hide_index=True
    )

# --- TAB 3: REGIONAL EFFECTS ---
with tab3:
    st.subheader("Regional Price Premiums (Effect Sizes)")
    st.markdown("Percentage impact on baseline price for a property located in a specific region, holding other features (rooms, area) constant.")
    
    eff_25 = data['effect_sizes_percentage']['2025']
    eff_26 = data['effect_sizes_percentage']['2026']
    
    # Extract just the regions
    regions = [k for k in eff_26.keys() if "inferred_region" in k]
    df_regions = pd.DataFrame({
        "Region": [clean_feature_name(r) for r in regions],
        "Premium_2025": [eff_25.get(r, 0) for r in regions],
        "Premium_2026": [eff_26.get(r, 0) for r in regions]
    })
    
    df_regions = df_regions.sort_values(by="Premium_2026", ascending=False)
    
    fig_reg = go.Figure()
    fig_reg.add_trace(go.Bar(x=df_regions["Region"], y=df_regions["Premium_2025"], name='2025', marker_color='#3498db'))
    fig_reg.add_trace(go.Bar(x=df_regions["Region"], y=df_regions["Premium_2026"], name='2026', marker_color='#fa8c05'))
    fig_reg.update_layout(
        barmode='group', 
        yaxis_title="Price Premium (%) relative to baseline", 
        xaxis_tickangle=-45,
        margin=dict(b=100)
    )
    
    st.plotly_chart(fig_reg, use_container_width=True)