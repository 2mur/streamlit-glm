import pandas as pd
import ast
import glob
import os
import re

# Local Paths:
DATASETS_DIR = '2025-datasets'
LISTINGS_DIR = '2025-listings'
CLEAN_DIR = '2025-clean-datasets'
FINAL_OUTPUT = 'cleaned-properties-data-2025.csv'

def rundf1():
    df1 = pd.read_csv(f'{DATASETS_DIR}/centris-dataset-1-n-5000.csv')
    newdf1 = []
    n=0
    for _, row in df1.iterrows():
        newdict = {}
        try:
            url = row['URL']
            newdict['listing_id'] = url.split('~')[2].split('/')[1]
            newdict['region'] = url.split('~')[2].split('/')[0].split('montreal-')[-1]
            
            newdict['address'] = row['Address']

            adress_comp = str(row['Address']).split(':')
            
            newdict['address'] = adress_comp[0] 
            if len(adress_comp) == 3:
                quartier = adress_comp[-1].split('Neighbourhood')[-1]
                newdict['quartier'] = quartier.strip()
            else:
                quartier = adress_comp[-1].split('(')[-1]
                newdict['quartier'] = quartier[:-1]

            newdict['category'] = url.split('~')[0].split('/')[-1]


            newdict['price'] = int(row['Price'].replace("$", "").replace(",", ""))
            newdict['bedrooms'] = int(row['Bedrooms'])
            newdict['bathrooms'] = int(row['Bathrooms'])
            newdict['data_id'] = 'cd1'

            newdf1.append(newdict)
        except Exception as e:
            n = n+1
            continue

    print(f"Skipped {n} rows due to errors in dataset 1.")
    newdf1 = pd.DataFrame(newdf1)
    newdf1.to_csv(f'{CLEAN_DIR}/centris-dataset-1-clean.csv', index=False)

def rundf2():
    df = pd.read_csv(f'{DATASETS_DIR}/centris-dataset-2-n-700.csv')
    newdf = []
    n=0

    for _, row in df.iterrows():
        newdict = {}
        url = row['url']
        newdict['listing_id'] = url.split('~')[2].split('/')[1]
        newdict['region'] = url.split('~')[2].split('/')[0].split('montreal-')[-1]

        address = row['address']

        if pd.isna(address):
            newdict['quartier'] = None
        elif 'Neighbourhood' in str(address):
            quartier = str(address).split('Neighbourhood')[-1]
            newdict['quartier'] = quartier.strip()
        elif '(' in str(address):
            quartier = str(address).split('(')[-1]
            newdict['quartier'] = quartier[:-1]
        else:
            quartier = str(address).split(',')[-1].strip()
            newdict['quartier'] = quartier

        newdict['category'] = url.split('~')[0].split('/')[-1]
        
        if newdict['category'] == 'condos':
            newdict['address'] = "".join(str(address).split(',')[:3])
        else: 
            newdict['address'] = "".join(str(address).split(',')[:2])

        if pd.isna(row['price']):
            newdict['price'] = None
        else:
            newdict['price'] = int(str(row['price']).replace("$", "").replace(",", ""))
        
        if pd.isna(row['total_rooms']):
            newdict['total_rooms'] = None
        else:
            newdict['total_rooms'] = int(str(row['total_rooms']).split('room')[0])

        if pd.isna(row['bedrooms']):
            newdict['bedrooms'] = None
        else:
            newdict['bedrooms'] = int(str(row['bedrooms']).split('bedroom')[0])

        if pd.isna(row['bedrooms']):
            newdict['bathrooms']= None
        elif 'bathroom' in str(row['bathrooms']):
            newdict['bathrooms'] = int(str(row['bathrooms']).split('bathroom')[0].strip())
        else:
            newdict['bathrooms']= None

        value = ['Living area', 'Net area', 'Lot area', 'Gross area']
        keys = ['live-area', 'net-area', 'lot-area', 'gr-area']
        dict_areas = dict(zip(keys,value))
        for k, v in dict_areas.items():
            if pd.isna(row.get(v, pd.NA)):
                newdict[k] = None
            else:
                newdict[k] = int(str(row[v]).replace(',', '').split()[0])
        
        newdict['data_id'] = 'cd2'
        newdf.append(newdict)

    print(f"Skipped {n} rows due to errors in dataset 2.")
    newdf = pd.DataFrame(newdf)
    newdf.to_csv(f'{CLEAN_DIR}/centris-dataset-2-clean.csv', index=False)

def rundf3():
    df = pd.read_csv(f'{DATASETS_DIR}/centris-dataset-3-n-808.csv')
    newdf = []
    n = 0
    for _ , row in df.iterrows():
        try:
            row = row.to_dict()
            row = ast.literal_eval(row['listings'])
        except Exception as e:
            n += 1
            continue

        newdict = {}
        url = row['url']
        newdict['listing_id'] = url.split('~')[2].split('/')[1]
        newdict['region'] = url.split('~')[2].split('/')[0].split('montreal-')[-1]
        newdict['category'] = url.split('~')[0].split('/')[-1]

        address = row['address']
        newdict['address'] = address

        if newdict['category'] == 'condos':
            newdict['address'] = "".join(str(address).split(',')[:3])
        else: 
            newdict['address'] = "".join(str(address).split(',')[:2])
        
        if pd.isna(address):
            newdict['quartier'] = None
        elif 'Neighbourhood' in str(address):
            quartier = str(address).split('Neighbourhood')[-1]
            newdict['quartier'] = quartier.strip()
        elif '(' in str(address):
            quartier = str(address).split('(')[-1]
            newdict['quartier'] = quartier[:-1]
        else:
            quartier = str(address).split(',')[-1].strip()
            newdict['quartier'] = quartier

        if pd.isna(row['price']):
            newdict['price'] = None
        else:
            newdict['price'] = int(str(row['price']).replace("$", "").replace(",", ""))
        
        if pd.isna(row['total_rooms']):
            newdict['total_rooms'] = None
        else:
            newdict['total_rooms'] = int(str(row['total_rooms']).split('room')[0])

        if pd.isna(row['bedrooms']):
            newdict['bedrooms'] = None
        else:
            newdict['bedrooms'] = int(str(row['bedrooms']).split('bedroom')[0])

        if pd.isna(row['bedrooms']):
            newdict['bathrooms']= None
        elif 'bathroom' in str(row['bathrooms']):
            newdict['bathrooms'] = int(str(row['bathrooms']).split('bathroom')[0].strip())
        else:
            newdict['bathrooms']= None

        value = ['Living area', 'Net area', 'Lot area', 'Gross area']
        keys = ['live-area', 'net-area', 'lot-area', 'gr-area']
        dict_areas = dict(zip(keys,value))
        for k, v in dict_areas.items():
            if v not in row.keys():
                newdict[k] = None
            elif pd.isna(row[v]):
                newdict[k] = None
            else:
                newdict[k] = int(str(row[v]).replace(',', '').split()[0])
        newdict['data_id'] = 'cd3'
        newdf.append(newdict)

    print(f"Skipped {n} rows due to errors in dataset 3.")
    newdf = pd.DataFrame(newdf)
    newdf.to_csv(f'{CLEAN_DIR}/centris-dataset-3-clean.csv', index=False)

def rundf4():
    df = pd.read_csv(f'{DATASETS_DIR}/centris-dataset-4-n-99.csv')
    newdf = []
    n=0

    for _ , row in df.iterrows():
        newdict = {}
        url = row['url']
        newdict['listing_id'] = url.split('~')[2].split('/')[1]
        newdict['region'] = url.split('~')[2].split('/')[0].split('montreal-')[-1]

        address = row['address']
        newdict['address'] = address

        if pd.isna(address):
            newdict['quartier'] = None
        elif 'Neighbourhood' in str(address):
            quartier = str(address).split('Neighbourhood')[-1]
            newdict['quartier'] = quartier.strip()
        elif '(' in str(address):
            quartier = str(address).split('(')[-1]
            newdict['quartier'] = quartier[:-1]
        else:
            quartier = str(address).split(',')[-1].strip()
            newdict['quartier'] = quartier

        newdict['category'] = url.split('~')[0].split('/')[-1]
        if newdict['category'] == 'condos':
            newdict['address'] = "".join(str(address).split(',')[:3])
        else: 
            newdict['address'] = "".join(str(address).split(',')[:2])

        if pd.isna(row['price']):
            newdict['price'] = None
        else:
            newdict['price'] = int(str(row['price']).replace("$", "").replace(",", ""))
        
        if pd.isna(row['total_rooms']):
            newdict['total_rooms'] = None
        else:
            newdict['total_rooms'] = int(str(row['total_rooms']).split('room')[0])

        if pd.isna(row['bedrooms']):
            newdict['bedrooms'] = None
        else:
            newdict['bedrooms'] = int(str(row['bedrooms']).split('bedroom')[0])

        if pd.isna(row['bedrooms']):
            newdict['bathrooms']= None
        elif 'bathroom' in str(row['bathrooms']):
            newdict['bathrooms'] = int(str(row['bathrooms']).split('bathroom')[0].strip())
        else:
            newdict['bathrooms']= None

        value = ['Living area', 'Net area', 'Lot area', 'Gross area']
        keys = ['live-area', 'net-area', 'lot-area', 'gr-area']
        dict_areas = dict(zip(keys,value))
        for k, v in dict_areas.items():
            if pd.isna(row.get(v, pd.NA)):
                newdict[k] = None
            else:
                newdict[k] = int(str(row[v]).replace(',', '').split()[0])

        newdict['data_id'] = 'cd4'
        newdf.append(newdict)

    print(f"Skipped {n} rows due to errors in dataset 4.")
    newdf = pd.DataFrame(newdf)
    newdf.to_csv(f'{CLEAN_DIR}/centris-dataset-4-clean.csv', index=False)

def rundf5():
    df = pd.read_csv(f'{DATASETS_DIR}/centris-dataset-5-n-901.csv')
    newdf = []
    n = 0
    for _ , row in df.iterrows():
        try:
            row = row.to_dict()
            row = ast.literal_eval(row['listings'])
        except Exception as e:
            n += 1
            continue

        newdict = {}
        url = row['url']
        newdict['listing_id'] = url.split('~')[2].split('/')[1]
        newdict['region'] = url.split('~')[2].split('/')[0].split('montreal-')[-1]
        newdict['category'] = url.split('~')[0].split('/')[-1]
        
        address = row['address']
        newdict['address'] = address

        if newdict['category'] == 'condos':
            newdict['address'] = "".join(str(address).split(',')[:3])
        else: 
            newdict['address'] = "".join(str(address).split(',')[:2])

        if pd.isna(address):
            newdict['quartier'] = None
        elif 'Neighbourhood' in str(address):
            quartier = str(address).split('Neighbourhood')[-1]
            newdict['quartier'] = quartier.strip()
        elif '(' in str(address):
            quartier = str(address).split('(')[-1]
            newdict['quartier'] = quartier[:-1]
        else:
            quartier = str(address).split(',')[-1].strip()
            newdict['quartier'] = quartier

        if pd.isna(row['price']):
            newdict['price'] = None
        else:
            newdict['price'] = int(str(row['price']).replace("$", "").replace(",", ""))
        
        if pd.isna(row['total_rooms']):
            newdict['total_rooms'] = None
        else:
            newdict['total_rooms'] = int(str(row['total_rooms']).split('room')[0])

        if pd.isna(row['bedrooms']):
            newdict['bedrooms'] = None
        else:
            newdict['bedrooms'] = int(str(row['bedrooms']).split('bedroom')[0])

        if pd.isna(row['bedrooms']):
            newdict['bathrooms']= None
        elif 'bathroom' in str(row['bathrooms']):
            newdict['bathrooms'] = int(str(row['bathrooms']).split('bathroom')[0].strip())
        else:
            newdict['bathrooms']= None

        value = ['Living area', 'Net area', 'Lot area', 'Gross area']
        keys = ['live-area', 'net-area', 'lot-area', 'gr-area']
        dict_areas = dict(zip(keys,value))
        for k, v in dict_areas.items():
            if v not in row.keys():
                newdict[k] = None
            elif pd.isna(row[v]):
                newdict[k] = None
            else:
                newdict[k] = int(str(row[v]).replace(',', '').split()[0])
                
        newdict['data_id'] = 'cd5'
        newdf.append(newdict)

    print(f"Skipped {n} rows due to errors in dataset 5.")
    newdf = pd.DataFrame(newdf)
    newdf.to_csv(f'{CLEAN_DIR}/centris-dataset-5-clean.csv', index=False)

def concatenate_datasets():
    used_ids = []
    newdf = []
    d = 0
    for i in range(1,6):
        print(f"Processing dataset {i}")
        i = 6-i
        df = pd.read_csv(f'{CLEAN_DIR}/centris-dataset-{i}-clean.csv')
        for _, row in df.iterrows():

            if row['listing_id'] in used_ids:
                d = d+1
                continue
            else:
                used_ids.append(row['listing_id'])
                newdf.append(row)

    print(f"Skipped {d} rows due to duplicate IDs.")
    pd.DataFrame(newdf).to_csv(f'{CLEAN_DIR}/clean-centris-datasets-2025-1.csv', index=False)

def concat_region_datasets():
    newdf = []
    
    file_paths = glob.glob(os.path.join(LISTINGS_DIR, '*_listings.csv'))

    if not file_paths:
        print(f"No listing files found locally in {LISTINGS_DIR}")
        return

    for file_path in file_paths:
        print(f"Processing file: {file_path}")
        df = pd.read_csv(file_path)
        
        for _, row in df.iterrows():
            newdict = {}
            url = row['url']
            newdict['listing_id'] = url.split('~')[2].split('/')[1]
            newdict['region'] = url.split('~')[2].split('/')[0].split('montreal-')[-1]
            newdict['category'] = url.split('~')[0].split('/')[-1]

            address = row['address']
            if newdict['category'] == 'condos':
                newdict['address'] = "".join(str(address).split(',')[:3])
            else: 
                newdict['address'] = "".join(str(address).split(',')[:2])

            if pd.isna(address):
                newdict['quartier'] = None
            elif 'Neighbourhood' in str(address):
                quartier = str(address).split('Neighbourhood')[-1]
                newdict['quartier'] = quartier.strip()
            elif '(' in str(address):
                quartier = str(address).split('(')[-1]
                newdict['quartier'] = quartier[:-1]
            else:
                quartier = str(address).split(',')[-1].strip()
                newdict['quartier'] = quartier

            if pd.isna(row['price']):
                newdict['price'] = None
            elif isinstance(row['price'], (float, int)):
                newdict['price'] = int(row['price'])
            else:
                newdict['price'] = int(str(row['price']).split('price')[0].replace("$", "").replace(",", ""))
            
            if pd.isna(row['total_rooms']):
                newdict['total_rooms'] = None
            elif 'room' in str(row['total_rooms']):
                newdict['total_rooms'] = int(str(row['total_rooms']).split('room')[0].strip())
            else:
                newdict['total_rooms'] = None

            if pd.isna(row['bedrooms']):
                newdict['bedrooms'] = None
            elif 'bedroom' in str(row['bedrooms']):
                newdict['bedrooms'] = int(str(row['bedrooms']).split('bedroom')[0].strip())
            else:
                newdict['bedrooms']= None

            if pd.isna(row['bathrooms']):
                newdict['bathrooms']= None
            elif 'bathroom' in str(row['bathrooms']):
                newdict['bathrooms'] = int(str(row['bathrooms']).split('bathroom')[0].strip())
            else:
                newdict['bathrooms']= None

            value = ['Living area', 'Net area', 'Lot area', 'Gross area']
            keys = ['live-area', 'net-area', 'lot-area', 'gr-area']
            dict_areas = dict(zip(keys, value))
            for k, v in dict_areas.items():
                if v not in row.keys():
                    newdict[k] = None
                elif pd.isna(row[v]):
                    newdict[k] = None
                else:
                    newdict[k] = int(str(row[v]).replace(',', '').split()[0])

            # Extract Coordinates
            newdict['latitude'] = row['latitude'] if 'latitude' in row.keys() and pd.notna(row['latitude']) else None
            newdict['longitude'] = row['longitude'] if 'longitude' in row.keys() and pd.notna(row['longitude']) else None
            newdict['data_id'] = 'r'
            newdf.append(newdict)

    # Convert to DataFrame
    final_df = pd.DataFrame(newdf)
    
    # Deduplicate and prioritize coordinates
    if not final_df.empty:
        final_df['has_coords'] = final_df['latitude'].notna() & final_df['longitude'].notna()
        final_df = final_df.sort_values(by='has_coords', ascending=False)
        final_df = final_df.drop_duplicates(subset=['listing_id'], keep='first')
        final_df = final_df.drop(columns=['has_coords'])

    final_df.to_csv(f'{CLEAN_DIR}/clean-centris-datasets-2025-2.csv', index=False)

def final_cat():
    used_ids = []
    newdf = []
    d = 0

    rdf = pd.read_csv(f'{CLEAN_DIR}/clean-centris-datasets-2025-1.csv')
    cdf = pd.read_csv(f'{CLEAN_DIR}/clean-centris-datasets-2025-2.csv')
    used_ids = rdf['listing_id'].to_list()
    
    for _, row in cdf.iterrows():
        if row['listing_id'] in used_ids:
            d = d+1
            continue
        else:
            newdf.append(row)
            
    newdf = pd.DataFrame(newdf)
    fdf = pd.concat([rdf, newdf], ignore_index=True).sort_values(by='region')

    # --- Address & Quartier Cleaning ---
    fdf['address'] = fdf['address'].astype(str)
    fdf['quartier'] = fdf['quartier'].astype(str)

    # 5. Clean up quartier typos and lingering words
    fdf['quartier'] = fdf['quartier'].str.replace('Neighbourhood', '', regex=False).str.strip()
    fdf['quartier'] = fdf['quartier'].str.replace(r'(?i)^Westmoun$', 'Westmount', regex=True)
    fdf['quartier'] = fdf['quartier'].str.replace(r'(?i)^Mont-roya$', 'Mont-royal', regex=True)
    fdf['quartier'] = fdf['quartier'].replace(['nan', 'None', ''], pd.NA)

    # Calculate the best area and condense the columns
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

    fdf['area'] = fdf.apply(get_best_area, axis=1)

    # Drop the original redundant area columns
    fdf = fdf.drop(columns=['live-area', 'net-area', 'lot-area', 'gr-area'])

    fdf.to_csv(FINAL_OUTPUT, index=False)

    print(f"Final combined dataset saved to: {FINAL_OUTPUT}")

    # Print Diagnostics
    print("Percentage of null values per column:")
    null_percentages = fdf.isnull().mean() * 100
    print(null_percentages.round(2).astype(str) + ' %')
    print("-" * 30)

# To run the pipeline:
rundf1()
rundf2()
rundf3()
rundf4()
rundf5()
concatenate_datasets()
concat_region_datasets()
final_cat()