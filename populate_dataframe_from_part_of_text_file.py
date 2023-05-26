import pandas as pd
import os

in_file = r'C:\Users\jasonwalker\OneDrive - SitusAMC\Documents\project\baseline\history_file\vectors_december_small.csv'
out_file = r'c:\temp\df_test.xlsx'

df = pd.read_csv(in_file, nrows=100, header=None)
df.to_excel(out_file, index=False)
os.startfile(out_file)












