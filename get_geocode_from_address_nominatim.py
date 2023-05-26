import os

from geopy.geocoders import Nominatim
import time
import pandas as pd

out_file = r'c:\temp\out1.xlsx'

addresses = [
     'CEDAR CITY, UT'
    , 'HEBER, UT'
    , 'OGDEN, UT'
    , 'PRICE, UT'
]

property_geocode = []
columns = ['ADDRESS', 'LATITUDE', 'LONGITUDE']

for a in addresses:
    loc = a
    geolocator = Nominatim(user_agent='SAMC')
    location = geolocator.geocode(loc)

    if location:
        property_geocode.append([loc, location.latitude, location.longitude])

    time.sleep(2)

property_geocode_df = pd.DataFrame(data=property_geocode, columns=columns)
property_geocode_df.to_excel(out_file)
os.startfile(out_file)

