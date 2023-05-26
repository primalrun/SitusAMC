import os
import requests
import json
import pandas as pd
import sys

overpass_url = r'http://overpass-api.de/api/interpreter'
out_file = r'c:\temp\out1.xlsx'
country = 'UNITED STATES'
lat = 17.980045
long = -66.1515668546581
data_exist = True

# region_lat_long = {
#     'brisbane_australia': (-27.4705, 153.0260)
#     , 'melbourne_australia': (-37.8136, 144.9631)
#     , 'sydney_australia': (-33.8688, 151.2093)
#     , 'montreal_canada': (45.5017, -73.5673)
#     , 'toronto_canada': (43.6532, -79.3832)
#     , 'vancouver_canada': (49.2827, -123.1207)
#     , 'biarritz_france': (43.4832, -1.5586)
#     , 'montpellier_france': (43.6108, 3.8767)
#     , 'orleans_france': (47.9030, 1.9093)
#     , 'paris_france': (48.8566, 2.3522)
#     , 'toulouse_france': (43.6047, 1.4442)
#     , 'bielefeld_germany': (52.0302, 8.5325)
#     , 'frankfurt_germany': (50.1109, 8.6821)
#     , 'gottingen_germany': (51.5413, 9.9158)
#     , 'hamburg_germany': (53.5511, 9.9937)
#     , 'heidelberg_germany': (49.3988, 8.6724)
#     , 'leipzip_germany': (51.3397, 12.3731)
#     , 'munich_germany': (48.1351, 11.5820)
#     , 'stuttgart_germany': (48.7758, 9.1829)
#     , 'unknown_ireland': (53.1424, -7.6921)
#     , 'bologna_italy': (44.4949, 11.3426)
#     , 'campobasso_italy': (41.5603, 14.6627)
#     , 'florence_italy': (43.7696, 11.2558)
#     , 'milan_italy': (45.4642, 9.1900)
#     , 'rome_italy': (41.9028, 12.4964)
#     , 'cabo san lucas_mexico': (22.8905, -109.9167)
#     , 'guadalajara_mexico': (20.6597, -103.3496)
#     , 'mexico city_mexico': (19.4326, -99.1332)
#     , 'amsterdam_netherlands': (52.3676, 4.9041)
#     , 'rotterdam_netherlands': (51.9244, 4.4777)
#     , 'the hague_netherlands': (52.0705, 4.3007)
#     , 'utrecht_netherlands': (52.0907, 5.1214)
#     , 'barcelona_spain': (41.3874, 2.1686)
#     , 'bilbao_spain': (43.2630, -2.9350)
#     , 'madrid_spain': (40.4168, -3.7038)
#     , 'valencia_spain': (39.4699, -0.3763)
#     , 'bristol_united kingdom': (51.4545, -2.5879)
#     , 'leeds_united kingdom': (53.8008, -1.5491)
#     , 'london_united kingdom': (51.5072, -0.1276)
#     , 'manchester_united kingdom': (53.4808, -2.2426)
#     , 'austin_united states': (30.2672, -97.7431)
#     , 'chicago_united states': (41.8781, -87.6298)
#     , 'new york_united states': (40.7128, -74.0060)
#     , 'san francisco_united states': (37.7749, -122.4194)
# }


def get_property_data(property_row):
    source_id = property_row['id']
    tags = property_row['tags']
    if 'center' in property_row:
        latitude = property_row['center']['lat']
        longitude = property_row['center']['lon']
    else:
        latitude = property_row['lat']
        longitude = property_row['lon']
    return [source_id, latitude, longitude, tags]


def demo_classification(tags):
    if 'tourism' in tags:
        if tags['tourism'] == 'museum':
            return 'MUSUEM'
    elif 'amenity' in tags:
        if tags['amenity'] == 'hospital':
            return 'HOSPITAL'
    else:
        return 'OTHER'


def property_attributes(tags):
    if 'name' in tags:
        prop_name = tags['name']
    else:
        prop_name = None
    if 'addr:housenumber' in tags:
        prop_street_number = tags['addr:housenumber']
    else:
        prop_street_number = None
    if 'addr:street' in tags:
        prop_street_name = tags['addr:street']
    else:
        prop_street_name = None
    if 'addr:city' in tags:
        prop_city = tags['addr:city']
    elif 'addr:suburb' in tags:
        prop_city = tags['addr:suburb']
    else:
        prop_city = None
    if 'addr:state' in tags:
        prop_state = tags['addr:state']
    else:
        prop_state = None
    if 'addr:postcode' in tags:
        prop_postcode = tags['addr:postcode']
    else:
        prop_postcode = None
    return [prop_name, prop_street_number, prop_street_name, prop_city, prop_state, prop_postcode]


def make_address(row):
    if row.STREET_NUMBER is None:
        return row.STREET_NAME
    else:
        return str(row.STREET_NUMBER) + ' ' + str(row.STREET_NAME)


result_columns = ['SOURCE_ID'
                   , 'DEMO_CLASSIFICATION'
                   , 'PROPERTY_NAME'
                   , 'ADDRESS'
                   , 'CITY'
                   , 'STATE'
                   , 'POSTCODE'
                   , 'COUNTRY'
                   , 'LATITUDE'
                   , 'LONGITUDE'
                   ]

overpass_query = (
    r'[out:json][timeout:180];'
    r'('
    f'nwr(around:10000,{lat}, {long})["amenity"="hospital"];'
    f'nwr(around:10000,{lat}, {long})["tourism"="museum"];'
    r');'
    r'out center;'
)

response = requests.get(overpass_url,
                        params={'data': overpass_query})

if response:
    address_data = response.json()
else:
    response.raise_for_status()
    print(response.status_code)

elements = address_data['elements']

# source_id, latitude, longitude, address data
address_1 = [get_property_data(x) for x in elements]

# only addresses with street address
# demo_classification, source_id, latitude, longitude, address data
address_2 = [[demo_classification(x[3])] + x for x in address_1 if 'addr:street' in x[3]]


city_set = set([property_attributes(x[4])[3] for x in address_2 if property_attributes(x[4])[3] is not None])
zip_set = set([property_attributes(x[4])[5] for x in address_2 if property_attributes(x[4])[5] is not None])

if not city_set or not zip_set:
    print('either city or zip is only NULL')
    sys.exit()

if len(address_2) == 0:
    print(f'No matching addresses for {country}, {lat}, {long}')
    # return empty dataframe if no addresses
    data_exist = False
    df_result = pd.DataFrame(data=[], columns=result_columns)
    sys.exit()

address_3 = []

for x in address_2:
    # demo_classification, source_id, latitude, longitude, address attributes
    address_3.append([x[0], x[1], x[2], x[3]] + property_attributes(x[4]))


columns = [
    'DEMO_CLASSIFICATION'
    , 'SOURCE_ID'
    , 'LATITUDE'
    , 'LONGITUDE'
    , 'PROPERTY_NAME'
    , 'STREET_NUMBER'
    , 'STREET_NAME'
    , 'CITY'
    , 'STATE'
    , 'POSTCODE'
]
address_df = pd.DataFrame(data=address_3, columns=columns)
address_df['PROPERTY_NAME'] = address_df['PROPERTY_NAME'].str.upper()
address_df['STREET_NAME'] = address_df['STREET_NAME'].str.upper()
address_df['CITY'] = address_df['CITY'].str.upper()
address_df['STATE'] = address_df['STATE'].str.upper()

# inlcude addresses with CITY and POSTCODE
address_df_filter_1 = address_df.dropna(subset=['CITY', 'POSTCODE']).copy()

# unique STATE value
state_unique = address_df_filter_1.STATE.dropna().unique()

# populate missing STATE value if all existing values are 1 value
if len(state_unique) == 1:
    state = state_unique[0]
    state_unique_column = [state for x in range(0, len(address_df_filter_1.index))]
    address_df_filter_1 = address_df_filter_1.assign(STATE=state_unique_column)


# add ADDRESS column, STREET_NUMBER + STREET_NAME
address_df_filter_1.loc[:, 'ADDRESS'] = address_df_filter_1.apply(make_address, axis=1)

# add COUNTRY column
country_column = [country for x in range(0, len(address_df_filter_1.index))]
address_df_filter_1.loc[:, 'COUNTRY'] = country_column

# drop duplicate ADDRESS and CITY
df_dedupe_1 = address_df_filter_1.drop_duplicates(subset=['ADDRESS', 'CITY'], keep='last').reset_index(drop=True)




df_result = df_dedupe_1[result_columns]
df_result.to_excel(out_file, index=False)
os.startfile(out_file)
