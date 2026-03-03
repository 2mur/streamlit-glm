import pandas as pd
import numpy as np
import json

input_file = 'cleaned-properties-timeseries-geoencoded.csv'
output_file = 'dashboard_data.json'

df = pd.read_csv(input_file)
print(len(df))

df["category"] = df["category"].str.lower()
df.loc[df["category"].isin(["lofts-studios"]), "category"] = "condos"
df = df[~df["category"].isin(["cottages", "mobile-homes"])]
print(len(df))

df = df[
    (df["bedrooms"].isna() | (df["bedrooms"] <= 20)) &
    (df["bathrooms"].isna() | (df["bathrooms"] <= 20))
]
print(len(df))
df = df[
    (df["price_2025"] > 50000) |
    (df["price_2026"] > 50000)
]
print(len(df))

df = df[
    (df["area"] > 200) | (df["area"].isna())
]
print(len(df))

# Define 250k bins up to 10M
bins = np.arange(0, 10250000, 250000)
labels = [f"${i/1000000}M-{(i+250000)/1000000}M" for i in bins[:-1]]
labels[-1] = ">$10.0M"
bins = np.append(bins, np.inf)


def get_metrics(data, price_col):
    metrics = {'categories': {}, 'total_count': len(data)}
    if data.empty: return metrics
    
    data = data.copy()
    data['psqft'] = np.where(data['area'] > 0, data[price_col] / data['area'], np.nan)
    
    for cat, group in data.groupby('category'):
        hist, _ = np.histogram(group[price_col].fillna(0), bins=bins)
        metrics['categories'][cat] = {
            'count': len(group),
            'avg_price': float(group[price_col].mean()),
            'avg_psqft': float(group['psqft'].mean()) if not group['psqft'].isna().all() else 0,
            'histogram': hist.tolist()
        }
    return metrics

def calculate_changes(data_26, data_25, matched_data):
    changes = {}
    cats_26 = data_26['categories']
    cats_25 = data_25['categories']
    
    all_categories = set(cats_26.keys()).union(set(cats_25.keys()))
    
    for cat in all_categories:
        cat_changes = {}
        # Overall YoY Change
        if cat in cats_26 and cat in cats_25 and cats_25[cat]['avg_price'] > 0:
            cat_changes['yoy_avg_price'] = (cats_26[cat]['avg_price'] - cats_25[cat]['avg_price']) / cats_25[cat]['avg_price']
        else:
            cat_changes['yoy_avg_price'] = None

        # Matched YoY Change
        matched_cat = matched_data[matched_data['category'] == cat]
        if not matched_cat.empty:
            matched_cat_clean = matched_cat[(matched_cat['price_2025'] > 0) & (matched_cat['price_2026'] > 0)]
            if not matched_cat_clean.empty:
                # Average of individual property percentage changes
                matched_pct = ((matched_cat_clean['price_2026'] - matched_cat_clean['price_2025']) / matched_cat_clean['price_2025']).mean()
                cat_changes['matched_change'] = float(matched_pct)
            else:
                cat_changes['matched_change'] = None
        else:
            cat_changes['matched_change'] = None
            
        changes[cat] = cat_changes
    return changes

def process_dataset(df_subset):
    # Filter valid prices per year
    df_25 = df_subset.dropna(subset=['price_2025']).copy()
    df_26 = df_subset.dropna(subset=['price_2026']).copy()
    df_matched = df_subset[df_subset['matched'] == True].copy()
    
    
    stats_25 = get_metrics(df_25, 'price_2025')
    stats_26 = get_metrics(df_26, 'price_2026')
    
    changes_26 = calculate_changes(stats_26, stats_25, df_matched)
    
    # Inject changes into the 2026 dictionary
    for cat in stats_26['categories']:
        if cat in changes_26:
            stats_26['categories'][cat].update(changes_26[cat])
            
    return {
        '2025': stats_25,
        '2026': stats_26
    }

# Build Master JSON
dashboard_data = {
    'labels': labels,
    'global': process_dataset(df),
    'regions': {}
}

# Process each region
for region, group in df.groupby('inferred_region'):
    if pd.notna(region):
        dashboard_data['regions'][region] = process_dataset(group)

with open(output_file, 'w') as f:
    json.dump(dashboard_data, f)

print(f"Dashboard data generated and saved to {output_file}")