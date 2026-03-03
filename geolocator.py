import pandas as pd
import geopandas as gpd
from thefuzz import process

# File paths
input_csv = 'cleaned-properties-timeseries.csv'
geojson_path = 'montreal_regions.geojson'
output_csv = 'cleaned-properties-timeseries-geoencoded.csv'

# Load data
df = pd.read_csv(input_csv)
gdf_poly = gpd.read_file(geojson_path)

target_poly_col = 'NOM' 

# 1. Separate rows with and without coordinates
df_coords = df.dropna(subset=['latitude', 'longitude']).copy()
df_missing = df[df['latitude'].isna() | df['longitude'].isna()].copy()

# 2. Convert to GeoDataFrame and perform Spatial Join
gdf_points = gpd.GeoDataFrame(
    df_coords, 
    geometry=gpd.points_from_xy(df_coords.longitude, df_coords.latitude),
    crs="EPSG:4326" 
)

if gdf_poly.crs != gdf_points.crs:
    gdf_poly = gdf_poly.to_crs(gdf_points.crs)

joined = gpd.sjoin(gdf_points, gdf_poly, how="left", predicate="within")

df_coords['inferred_region'] = joined[target_poly_col]
df_coords = df_coords.drop(columns=['geometry', 'index_right'], errors='ignore')

# 3. Extract the list of valid 'NOM' regions from the GeoJSON
nom_list = gdf_poly[target_poly_col].dropna().unique().tolist()

# Define the fuzzy matching logic
def get_fuzzy_match(text, choices, threshold=80):
    if pd.isna(text) or not str(text).strip():
        return None
    
    # process.extractOne returns a tuple: (best_match_string, score)
    match, score = process.extractOne(str(text), choices)
    
    if score >= threshold:
        return match
    return None

# 4. Apply fuzzy matching to infer missing regions
print("Running fuzzy matching for rows missing coordinates...")

# First attempt: Match against 'quartier'
df_missing['inferred_region'] = df_missing['quartier'].apply(
    lambda x: get_fuzzy_match(x, nom_list, threshold=80)
)

# Second attempt: If still missing, match against 'region'
df_missing['inferred_region'] = df_missing.apply(
    lambda row: get_fuzzy_match(row['region'], nom_list, threshold=80) 
    if pd.isna(row['inferred_region']) else row['inferred_region'], 
    axis=1
)

# 5. Recombine and save
final_df = pd.concat([pd.DataFrame(df_coords), df_missing], ignore_index=True)

final_df = final_df.sort_values(by=['matched', 'listing_id'], ascending=[False, True])
final_df = final_df.drop(columns=['address','region','quartier'])

final_df.to_csv(output_csv, index=False)
print(f"Region inference complete. Saved to {output_csv}")
print(f"Total rows: {len(final_df)}")
print(f"Rows permanently missing an inferred region: {final_df['inferred_region'].isna().sum()}")