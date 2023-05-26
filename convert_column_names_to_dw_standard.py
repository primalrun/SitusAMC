import pandas as pd
import os

in_file = r'C:\Users\jasonwalker\OneDrive - SitusAMC\Documents\project\baseline\data_elements\dirtyloans_csv_columns.txt'
out_file = r'c:\temp\column_dw_standard.txt'

df = pd.read_csv(in_file, header=None, names=['COLUMN'])
df['COLUMN'] = df['COLUMN'].str.replace(' ', '_').str.replace('-', '_').str.upper()
df.to_csv(out_file, index=False, header=False)
os.startfile(out_file)
