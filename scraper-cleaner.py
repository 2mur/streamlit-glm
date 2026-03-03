import pandas as pd
import re


# File paths (Local - uncomment to use locally)
input_path = 'raw-properties-data-2026.csv'
output_path = 'cleaned-properties-data-2026.csv'

df = pd.read_csv(input_path)

# 1. Filter target columns
target_columns = [
    'listing_id', 'input_url', 'price', 'property_type', 'address', 'lot_area', 'zoning', 
    'municipal_assessment_lot', 'latitude', 'longitude', 'description'
]
df = df[target_columns].copy()

# 2. Extract price
df['price'] = df['price'].astype(str).str.extract(r'"value"\s*:\s*(\d+)', expand=False).astype(float)

# 3. Clean municipal assessment
df['municipal_assessment_lot'] = df['municipal_assessment_lot'].astype(str).str.replace(r'[\$,]', '', regex=True)
df['municipal_assessment_lot'] = pd.to_numeric(df['municipal_assessment_lot'], errors='coerce')

# 4. Clean lot area
df['lot_area'] = df['lot_area'].astype(str).str.replace(',', '', regex=False)
df['lot-area'] = df['lot_area'].str.extract(r'(\d+(?:\.\d+)?)', expand=False).astype(float)

# 5. Parse description for Bedrooms and Bathrooms
text_to_num = {
    'one': 1.0, 'une': 1.0, 'a': 1.0, 'single': 1.0, 'two': 2.0, 'deux': 2.0, 'both': 2.0, '2x': 2.0,
    'three': 3.0, 'trois': 3.0, 'four': 4.0, 'quatre': 4.0, 'five': 5.0, 'cinq': 5.0,
    'six': 6.0, 'seven': 7.0, 'sept': 7.0, 'eight': 8.0, 'huit': 8.0,
    'nine': 9.0, 'neuf': 9.0, 'ten': 10.0, 'dix': 10.0, 'half': 0.5
}
filler = r'(?:\s+\w+){0,2}' 

# Bathrooms
bath_num = r'\b(\d+(?:½|\.\d+)?|one|two|three|four|five|seven|une|deux|trois|quatre|cinq|a|single|both|half|2x)\b'
bath_kw = r'\s+(?:bath(?:room|rooms|s)?|sdb|salle(?:s)? de bain(?:s)?)\b'
extracted_baths = df['description'].str.extract(bath_num + filler + bath_kw, flags=re.IGNORECASE)[0]
extracted_baths = extracted_baths.str.lower().str.replace('½', '.5')
df['bathrooms'] = extracted_baths.map(text_to_num).fillna(pd.to_numeric(extracted_baths, errors='coerce'))
has_bath_word = df['description'].str.contains(r'\b(?:bath(?:room|rooms|s)?|sdb|salle(?:s)? de bain(?:s)?)\b', flags=re.IGNORECASE, na=False)
df.loc[df['bathrooms'].isnull() & has_bath_word, 'bathrooms'] = 1.0

# Bedrooms
bed_num = r'\b(\d+|one|two|three|four|five|six|seven|eight|nine|ten|une|deux|trois|quatre|cinq|six|sept|huit|neuf|dix|a|single)\b'
bed_kw = r'\s+(?:bed(?:room|rooms|s)?|bdrm(?:s)?|bdr(?:s)?|chambre(?:s)?(?:\s+à\s+coucher)?|cac)\b'
extracted_beds = df['description'].str.extract(bed_num + filler + bed_kw, flags=re.IGNORECASE)[0]
extracted_beds = extracted_beds.str.lower()
df['bedrooms'] = extracted_beds.map(text_to_num).fillna(pd.to_numeric(extracted_beds, errors='coerce'))
has_bed_word = df['description'].str.contains(r'\b(?:bed(?:room|rooms|s)?|bdrm(?:s)?|bdr(?:s)?|chambre(?:s)?(?:\s+à\s+coucher)?|cac)\b', flags=re.IGNORECASE, na=False)
df.loc[df['bedrooms'].isnull() & has_bed_word, 'bedrooms'] = 1.0

# 6. Align Schema 

# Map categories
category_map = {
    'Condo for sale': 'condos',
    'Condominium house for sale': 'condominium-houses',
    'Condo for sale or for rent': 'condos',
    'Condominium house for sale or for rent': 'condominium-houses',
    'Loft / Studio for sale': 'condos',
    'House for sale': 'houses',
    'House for sale or for rent': 'houses',
    'Duplex for sale': 'duplexes',
    'Triplex for sale': 'triplexes',
    'Quadruplex for sale': '4plex',
    'Quintuplex for sale': '5plex',
    'Lot for sale': 'lots',
    'Lot for sale or for rent': 'lots',
    'Land for sale': 'land'
}
df['category'] = df['property_type'].map(category_map).fillna(df['property_type'])

# Extract region
df['region'] = df['input_url'].astype(str).str.split('~').str[-1]

# Extract quartier and clean address
def extract_quartier(address):
    if pd.isna(address): return None
    if 'Neighbourhood' in address: return address.split('Neighbourhood')[-1].strip()
    if '(' in address: return address.split('(')[-1].replace(')', '').strip()
    return address.split(',')[-1].strip()

df['quartier'] = df['address'].apply(extract_quartier)

# Clean address: drops everything starting from the comma before the city/borough parenthesis
df['address'] = df['address'].astype(str).str.replace(r',\s*[^,]+\s*\(.*', '', regex=True)
# Fallback catch for simple ", Montréal" strings without parentheses
df['address'] = df['address'].str.replace(r',\s*Montréal.*', '', regex=True, flags=re.IGNORECASE)

# Inject empty columns for the area function to evaluate
for col in ['total_rooms', 'live-area', 'net-area', 'gr-area']:
    df[col] = pd.NA

# 7. Finalize DataFrame & Calculate Area
def get_best_area(row):
    cat = str(row.get('category', '')).lower()
    areas = [row.get('live-area'), row.get('net-area'), row.get('lot-area'), row.get('gr-area')]
    valid_areas = [a for a in areas if pd.notna(a) and a > 0]
    
    if cat in ['lot', 'land']:
        if pd.notna(row.get('lot-area')) and row.get('lot-area') > 0:
            return row['lot-area']
        return max(valid_areas) if valid_areas else None
    else:
        if pd.notna(row.get('net-area')) and row.get('net-area') > 0:
            return row['net-area']
        if pd.notna(row.get('live-area')) and row.get('live-area') > 0:
            return row['live-area']
        if pd.notna(row.get('gr-area')) and row.get('gr-area') > 0:
            return row['gr-area']
        return None

df['area'] = df.apply(get_best_area, axis=1)

# Set the final column order and intrinsically drop intermediate columns (zoning, description, lot-area, etc.)
final_cols = [
    'listing_id', 'region', 'category', 'address', 'quartier', 'price', 
    'total_rooms', 'bedrooms', 'bathrooms', 'area', 
    'municipal_assessment_lot', 'latitude', 'longitude'
]
df = df[final_cols]

# 8. Save output
df.to_csv(output_path, index=False)
print(f"Data successfully aligned and saved to {output_path}")

# 9. Print Diagnostics
print("Percentage of null values per column:")
null_percentages = df.isnull().mean() * 100
print(null_percentages.round(2).astype(str) + ' %')
print("-" * 30)