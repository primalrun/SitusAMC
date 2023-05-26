import pandas as pd

source_file = r'C:\Temp\JASON_TEST.txt'
df = pd.read_csv(source_file, names=['ID'])

print(df)