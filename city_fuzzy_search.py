import pandas as pd
import pyodbc
import re
import os
import sys

# variable

# environment: (server, driver, uname, pword, dsn)
env_info = {'dev': (r'COM-DLKDWD01.SITUSAMC.COM', r'ODBC Driver 17 for SQL Server', r'python', r'FZ9H2Pcg=z%-cf?$',
                    r'DW_DEV_DW_LOAN_VALUATION')
    , 'uat': (r'COM-DWDBU01.SITUSAMC.COM', r'ODBC Driver 17 for SQL Server', r'python', r'FZ9H2Pcg=z%-cf?$',
              r'DW_UAT_DW_LOAN_VALUATION')
    , 'prod': (r'COM-DWAGP01.SITUSAMC.COM', r'SQL Server', r'python', r'FZ9H2Pcg=z%-cf?$', r'DW_PROD_DW_LOAN_VALUATION')
            }

env = env_info['dev']
db = r'DW_LOAN_VALUATION'
source_file = r'c:\temp\STATE_CITY.txt'


server = env[0]
driver = env[1]
uname = env[2]
pword = env[3]
dsn = env[4]



# function

def first_phrase(text):
    delimiter_count = [i for i, s in enumerate(text) if s == ' ']
    if len(delimiter_count) > 0:
        # return delimiter_count
        return text[:delimiter_count[0]]
    else:
        return text



# main program

conn01 = pyodbc.connect(
    f"Driver={driver};"
    f"Server={server};"
    f"Database={db};"
    f"UID={uname};"
    f"PWD={pword};"
    , autocommit=True)


sql_mlu = """
SELECT
	C.STATE_KEY
	,C.CITY_NAME AS MLU_CITY
FROM DW_DIMENSIONS.dbo.MLU_CITY C
LEFT JOIN DW_DIMENSIONS.dbo.MLU_STATE S
	ON C.STATE_KEY = S.STATE_KEY
WHERE
	C.COUNTRY_KEY = 100236
	AND NULLIF(C.CITY_NAME, '') IS NOT NULL
ORDER BY
	S.STATE_NAME
	,C.CITY_NAME
"""


df_source = pd.read_csv(filepath_or_buffer=source_file, names=['STATE_KEY', 'CITY'], delimiter='\t')
df_source['FIRST_PHRASE'] = df_source['CITY'].apply(lambda x: first_phrase(x))

df_mlu = pd.read_sql(sql=sql_mlu, con=conn01)
df_mlu['FIRST_PHRASE'] = df_mlu['MLU_CITY'].apply(lambda x: first_phrase(x))

df_combo = df_source.merge(right=df_mlu, how='left', on=['STATE_KEY', 'FIRST_PHRASE'])

df_combo.to_csv(path_or_buf=r'c:\temp\out.csv', index=False)
os.startfile(r'c:\temp\out.csv')



print('success')
