import pandas as pd

# File paths
file_2025 = 'cleaned-properties-data-2025.csv'
file_2026 = 'cleaned-properties-data-2026.csv'
output_file = 'cleaned-properties-timeseries.csv'

# Load datasets and aggressively suffix everything to prevent pandas auto-renaming
df_25 = pd.read_csv(file_2025).add_suffix('_25')
df_26 = pd.read_csv(file_2026).add_suffix('_26')

# --- Step 1: Merge on listing_id ---
matched_id = pd.merge(df_25, df_26, left_on='listing_id_25', right_on='listing_id_26', how='inner')
matched_id['matched'] = True
count_id_matches = len(matched_id)

# Extract unmatched rows
unmatched_25 = df_25[~df_25['listing_id_25'].isin(matched_id['listing_id_25'])]
unmatched_26 = df_26[~df_26['listing_id_26'].isin(matched_id['listing_id_26'])]

# --- Step 2: Merge unmatched rows on address ---
# Drop rows without addresses to prevent false positives on empty strings
unmatched_25_addr = unmatched_25.dropna(subset=['address_25'])
unmatched_26_addr = unmatched_26.dropna(subset=['address_26'])

matched_addr = pd.merge(unmatched_25_addr, unmatched_26_addr, left_on='address_25', right_on='address_26', how='inner')
matched_addr['matched'] = True
count_addr_matches = len(matched_addr)

# Extract completely unmatched rows and label them as False
final_unmatched_25 = unmatched_25[~unmatched_25['address_25'].isin(matched_addr['address_25'])].copy()
final_unmatched_26 = unmatched_26[~unmatched_26['address_26'].isin(matched_addr['address_26'])].copy()

final_unmatched_25['matched'] = False
final_unmatched_26['matched'] = False

# --- Step 3: Combine all segments ---
final_df = pd.concat([matched_id, matched_addr, final_unmatched_25, final_unmatched_26], ignore_index=True)

# --- Step 4: Coalesce Attributes ---
final_df['listing_id'] = final_df['listing_id_26'].combine_first(final_df['listing_id_25'])
final_df['address'] = final_df['address_26'].combine_first(final_df['address_25'])

# Map independent prices
final_df['price_2025'] = final_df['price_25']
final_df['price_2026'] = final_df['price_26']

# Use region from 2025 fallback to newer
final_df['region'] = final_df['region_25'].combine_first(final_df['region_26'])

# Coalesce other attributes
cols_prefer_26 = [
    'category', 'quartier', 'total_rooms', 'bedrooms', 
    'bathrooms', 'area', 'municipal_assessment_lot', 'latitude', 'longitude'
]

for col in cols_prefer_26:
    col_25 = f"{col}_25"
    col_26 = f"{col}_26"
    if col_25 in final_df.columns and col_26 in final_df.columns:
        final_df[col] = final_df[col_26].combine_first(final_df[col_25])

# --- Step 5: Enforce Integer Types ---
int_cols = ['listing_id', 'price_2025', 'price_2026', 'area']
for col in int_cols:
    if col in final_df.columns:
        final_df[col] = pd.to_numeric(final_df[col], errors='coerce').astype('Int64')

# --- Step 6: Filter missing prices & Calculate Diff ---
initial_count = len(final_df)
final_df = final_df.dropna(subset=['price_2025', 'price_2026'], how='all')
removed_count = initial_count - len(final_df)

# Calculate difference and create a temporary absolute column for sorting
final_df['price_diff'] = (final_df['price_2026'] - final_df['price_2025']).astype('Int64')
final_df['abs_price_diff'] = final_df['price_diff'].abs()

# Sort by matched first, then largest absolute price difference
final_df = final_df.sort_values(
    by=['matched', 'abs_price_diff', 'listing_id'], 
    ascending=[False, False, True],
    na_position='last'
)

# --- Step 7: Finalize Output ---
# Reorder columns and drop the intermediate suffixed/temp data
out_cols = [
    'listing_id', 'matched', 'address', 'region', 'quartier', 'category',
    'price_2025', 'price_2026', 'price_diff', 'total_rooms', 'bedrooms', 'bathrooms',
    'area', 'municipal_assessment_lot', 'latitude', 'longitude'
]
final_df = final_df[[c for c in out_cols if c in final_df.columns]]

# Save output
final_df.to_csv(output_file, index=False)

# Diagnostics Output
print(f"Matches found using listing_id: {count_id_matches}")
print(f"Matches found using address: {count_addr_matches}")
print(f"Rows removed due to missing prices: {removed_count}")
print(f"Total unique properties in dataset: {len(final_df)}")
print("-" * 30)
print("Missing value percentages per column:")
null_percentages = final_df.isnull().mean() * 100
print(null_percentages.round(2).astype(str) + ' %')