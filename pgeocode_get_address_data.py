import pgeocode
import pyodbc
import pandas as pd
import numpy as np
import os
import sys

country_alpha_2 = 'AU'
postcode = '3000'
stg_server = r'[AMC-DLKDWP01.amcfirst.com]'
stg_db = r'[DW_DIMENSIONS]'
out_file = r'c:\temp\get_state_from_zip.xlsx'

conn01 = pyodbc.connect(
    "Driver={SQL Server};"
    f"Server={stg_server[1:-1]};"
    f"Database={stg_db[1:-1]};"
    "UID=python;"
    "PWD=FZ9H2Pcg=z%-cf?$;"
    , autocommit=True)

sql_select = f"""
SELECT DISTINCT S.COUNTRY
	,C.[ALPHA-2_CODE] AS ALPHA2_CODE
	,S.POSTCODE
FROM DW_DIMENSIONS.stage.SRC_DEMO_PROPERTIES_OPENSTREETMAP S
INNER JOIN DW_DIMENSIONS.dbo.MLU_COUNTRY C ON S.COUNTRY = C.COUNTRY_NAME
WHERE STATE IS NULL
"""

df_columns = ['COUNTRY', 'ALPHA2_CODE', 'POSTCODE']
df_address = pd.read_sql(sql=sql_select, con=conn01, columns=df_columns)
add_state = []

for ind in df_address.index:
    country = df_address['COUNTRY'][ind]
    alpha2_code = df_address['ALPHA2_CODE'][ind]
    postcode = df_address['POSTCODE'][ind]
    nomi = pgeocode.Nominatim(alpha2_code)

    # pandas series wi
    ser = nomi.query_postal_code(postcode)
    state_nomi = ser['state_name']

    if not np.isnan(state_nomi):
        state = str(ser['state_name']).upper()
        add_state.append([country, alpha2_code, postcode, state])

# dataframe including STATE, without missing STATE values
df_columns = ['COUNTRY', 'ALPHA2_CODE', 'POSTCODE', 'STATE']
df_result = pd.DataFrame(data=add_state, columns=df_columns).dropna(subset=['STATE'])

# df_result.to_excel(out_file, index=False)
# os.startfile(out_file)

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
for index, row in df_result.iterrows():
    cursor.execute(sql_insert, row.COUNTRY, row.ALPHA2_CODE, row.POSTCODE, row.STATE)

cursor.close()
print('SUCCESS')
