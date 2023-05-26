import pyodbc
import pandas as pd

stg_server = r'[AMC-DLKDWP01.amcfirst.com]'
stg_db = r'[DW_DIMENSIONS]'
source_file = r'c:\temp\STATE_MANUAL.txt'

conn01 = pyodbc.connect(
    "Driver={SQL Server};"
    f"Server={stg_server[1:-1]};"
    f"Database={stg_db[1:-1]};"
    "UID=python;"
    "PWD=FZ9H2Pcg=z%-cf?$;"
    , autocommit=True)

df_source = pd.read_csv(source_file, sep='\t')
cursor = conn01.cursor()

sql_insert = """
INSERT INTO DW_DIMENSIONS.dbo.DEMO_COUNTRY_ZIP_MISSING (
COUNTRY
,ALPHA2_CODE
,POSTCODE
,STATE
)
VALUES(?, ?, ?, ?)
"""
for index, row in df_source.iterrows():
    cursor.execute(sql_insert, row.COUNTRY, row.ALPHA2_CODE, row.POSTCODE, row.STATE)

cursor.close()
print('SUCCESS')