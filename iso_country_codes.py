# https://www.nationsonline.org/oneworld/country_code_list.htm

import pandas as pd
import csv
import os

source_file = r'C:\Temp\ISO_COUNTRY_CODE.txt'
out_file = r'c:\temp\iso_country.xlsx'

with open(source_file, 'r') as f:
    reader = csv.reader(f)
    data = list(reader)

# only rows with 3 or more tabs
country = [str(x[0]).split(sep='\t') for x in data if str(x[0]).count('\t') >= 3]

# exclude flag if exists, last 4 columns included, if 5 columns exist, first column is flag
country_2 = [x[-4:] for x in country]

df_columns = ['COUNTRY'
              ,'ALPHA2'
              ,'ALPHA3'
              ,'UN_CODE']
df_country = pd.DataFrame(data=country_2, columns=df_columns)
df_country.to_excel(out_file, index=False)
os.startfile(out_file)


