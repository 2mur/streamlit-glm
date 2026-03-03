import pandas as pd
import numpy as np
import geopandas as gpd
import matplotlib.pyplot as plt
from scipy.interpolate import griddata

# File paths
input_csv = 'properties-with-regions.csv'
geojson_path = 'montreal_regions.geojson'

# Load data
df = pd.read_csv(input_csv)
gdf_poly = gpd.read_file(geojson_path)
target_poly_col = 'NOM'

# 1. Filter valid data and calculate $/sqft
df['target_price'] = df['price_2026'].combine_first(df['price_2025'])

# Keep rows with prices, areas, and inferred regions; exclude lots/land
df_plot = df.dropna(subset=['target_price', 'area', 'inferred_region']).copy()
df_plot = df_plot[~df_plot['category'].isin(['lot', 'land'])]
df_plot = df_plot[df_plot['area'] > 0]

df_plot['price_sqft'] = df_plot['target_price'] / df_plot['area']

# Remove extreme outliers globally before grouping
q_high = df_plot['price_sqft'].quantile(0.98)
df_plot = df_plot[df_plot['price_sqft'] <= q_high]

# 2. Group by region to calculate the average price per square foot
region_stats = df_plot.groupby('inferred_region')['price_sqft'].mean().reset_index()
region_stats.rename(columns={'price_sqft': 'mean_price_sqft'}, inplace=True)

# 3. Merge the statistical data with the geographic polygon data
gdf_poly = gdf_poly.merge(region_stats, left_on=target_poly_col, right_on='inferred_region', how='inner')

# 4. Calculate the geometric centroid of each region for the X/Y axes
# Project to a local UTM zone (Montreal is EPSG:32618) to calculate an accurate flat centroid, 
# then project back to standard GPS coordinates (EPSG:4326)
gdf_poly_proj = gdf_poly.to_crs(epsg=32618)
gdf_poly['centroid'] = gdf_poly_proj.centroid.to_crs(gdf_poly.crs)

x = gdf_poly['centroid'].x.values
y = gdf_poly['centroid'].y.values
z = gdf_poly['mean_price_sqft'].values

# 5. Create a 2D grid
grid_x, grid_y = np.mgrid[min(x):max(x):100j, min(y):max(y):100j]

# 6. Interpolate Z values over the grid using the regional centroids
grid_z = griddata((x, y), z, (grid_x, grid_y), method='linear')

# 7. Generate Matplotlib 3D Figure
fig = plt.figure(figsize=(12, 8))
ax = fig.add_subplot(111, projection='3d')

# Add the continuous interpolated surface
surf = ax.plot_surface(grid_x, grid_y, grid_z, cmap='viridis', alpha=0.8, edgecolor='none')

# Add the regional centroids as explicit scatter markers
ax.scatter(x, y, z, color='red', s=30, alpha=1, label='Regional Centroids')

# Formatting
ax.set_xlabel('Longitude')
ax.set_ylabel('Latitude')
ax.set_zlabel('Average Price / Sqft ($)')
ax.set_title('Montreal Regional Average: 3D Price per Sqft Heatmap')
fig.colorbar(surf, ax=ax, shrink=0.5, aspect=5, label='Mean Price / Sqft')

plt.legend()
plt.show()