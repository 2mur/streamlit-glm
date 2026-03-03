import pandas as pd
import numpy as np
import statsmodels.api as sm
import statsmodels.formula.api as smf
from scipy import stats
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.linear_model import LinearRegression
import shap
import json
import warnings
warnings.filterwarnings("ignore")


# Local fallbacks
INPUT_CSV = 'cleaned-properties-timeseries-geoencoded.csv'
OUTPUT_JSON = 'dashboard_inference_insights.json'

def prepare_data(filepath):
    df = pd.read_csv(filepath)
    
    # Clean Categories
    df["category"] = df["category"].str.lower()
    df.loc[df["category"] == "lofts-studios", "category"] = "condos"
    df = df[~df["category"].isin(["cottages", "mobile-homes"])].copy()

    # Define Units
    unit_map = {"houses": 2, "condos": 1, "condominium-houses": 1, "duplex": 2, 
                "triplex": 3, "4plex": 4, "5plex": 5, "lot": 0, "land": 0}
    df["units"] = df["category"].map(unit_map).fillna(1)

    # Imputation Strategy
    lot_mask = df["category"].isin(["lots", "land"])
    df.loc[lot_mask, ["total_rooms", "bedrooms", "bathrooms", "area"]] = 0

    df.loc[~lot_mask, "bathrooms"] = df.loc[~lot_mask, "bathrooms"].fillna(1)
    df.loc[~lot_mask, "bedrooms"] = df.loc[~lot_mask, "bedrooms"].fillna(1)
    df.loc[~lot_mask & df["area"].isna(), "area"] = df.loc[~lot_mask].groupby("category")["area"].transform(lambda x: x.fillna(x.median()))

    # Log Area Transform
    df["log_area"] = np.log(df["area"].replace(0, np.nan)).fillna(0)

    # Outlier & Price Filtering
    df = df[(df["bedrooms"].isna() | (df["bedrooms"] <= 20)) & 
            (df["bathrooms"].isna() | (df["bathrooms"] <= 20))]
    df = df[(df["price_2025"] > 50000) | (df["price_2026"] > 50000)]

    # Categorical Casts
    df["category"] = df["category"].astype("category")
    df["inferred_region"] = df["inferred_region"].astype("category")

    return df

def fit_glm(data, target_col):
    formula = f"{target_col} ~ bedrooms + bathrooms + log_area + units + C(category) + C(inferred_region)"
    model = smf.glm(
        formula=formula,
        data=data,
        family=sm.families.Gamma(link=sm.families.links.log())
    ).fit()
    return model

def calculate_drift(model_25, model_26):
    common_params = set(model_25.params.index).intersection(model_26.params.index)
    drift_results = []
    
    for param in common_params:
        b1, b2 = model_25.params[param], model_26.params[param]
        se1, se2 = model_25.bse[param], model_26.bse[param]
        
        z = (b1 - b2) / np.sqrt(se1**2 + se2**2)
        p_value = 2 * (1 - stats.norm.cdf(abs(z)))
        
        drift_results.append({
            "feature": param,
            "coef_2025": float(b1),
            "coef_2026": float(b2),
            "delta": float(b2 - b1),
            "z_score": float(z),
            "p_value": float(p_value),
            "significant": bool(p_value < 0.05)
        })
    
    return sorted(drift_results, key=lambda x: abs(x["z_score"]), reverse=True)

def run_shap_analysis(df_25, df_26, features):
    X_25, y_25 = df_25[features], np.log(df_25["price_2025"])
    X_26, y_26 = df_26[features], np.log(df_26["price_2026"])

    lin_25 = LinearRegression().fit(X_25, y_25)
    lin_26 = LinearRegression().fit(X_26, y_26)

    shap_25 = np.abs(shap.Explainer(lin_25, X_25)(X_25).values).mean(axis=0)
    shap_26 = np.abs(shap.Explainer(lin_26, X_26)(X_26).values).mean(axis=0)

    shap_drift = []
    for i, feat in enumerate(features):
        shap_drift.append({
            "feature": feat,
            "importance_2025": float(shap_25[i]),
            "importance_2026": float(shap_26[i]),
            "delta": float(shap_26[i] - shap_25[i])
        })
        
    return sorted(shap_drift, key=lambda x: abs(x["delta"]), reverse=True)

if __name__ == "__main__":
    print("Initializing pipeline...")
    df = prepare_data(INPUT_CSV)
    
    df_25 = df.dropna(subset=["price_2025"]).copy()
    df_26 = df.dropna(subset=["price_2026"]).copy()

    # Align categories
    df_26["category"] = df_26["category"].cat.set_categories(df_25["category"].cat.categories)
    df_26["inferred_region"] = df_26["inferred_region"].cat.set_categories(df_25["inferred_region"].cat.categories)

    print("Fitting GLM models...")
    model_25 = fit_glm(df_25, "price_2025")
    model_26 = fit_glm(df_26, "price_2026")

    print("Evaluating models...")
    pred_25 = model_25.predict(df_25)
    pred_26 = model_26.predict(df_26)

    print("Calculating coefficient and SHAP drift...")
    drift_stats = calculate_drift(model_25, model_26)
    shap_stats = run_shap_analysis(df_25, df_26, ["bedrooms", "bathrooms", "log_area", "units"])

    # Extract clean effect sizes (Exponentiated coefficients - 1)
    effect_25 = {k: float((np.exp(v) - 1) * 100) for k, v in model_25.params.items()}
    effect_26 = {k: float((np.exp(v) - 1) * 100) for k, v in model_26.params.items()}

    # Construct the JSON payload for the dashboard
    dashboard_payload = {
        "metadata": {
            "rows_2025": len(df_25),
            "rows_2026": len(df_26)
        },
        "performance": {
            "2025": {
                "mae": float(mean_absolute_error(df_25["price_2025"], pred_25)),
                "r2": float(r2_score(df_25["price_2025"], pred_25))
            },
            "2026": {
                "mae": float(mean_absolute_error(df_26["price_2026"], pred_26)),
                "r2": float(r2_score(df_26["price_2026"], pred_26))
            }
        },
        "effect_sizes_percentage": {
            "2025": effect_25,
            "2026": effect_26
        },
        "coefficient_drift": drift_stats,
        "shap_importance_drift": shap_stats
    }

    # Save to JSON
    with open(OUTPUT_JSON, 'w') as f:
        json.dump(dashboard_payload, f, indent=4)
        
    print(f"Pipeline complete. Insights exported to {OUTPUT_JSON}")