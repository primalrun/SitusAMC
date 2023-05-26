import pandas as pd
import pyodbc
import sys

env_info = {'dev': (r'COM-DLKDWD01.SITUSAMC.COM', r'ODBC Driver 17 for SQL Server', r'python', r'FZ9H2Pcg=z%-cf?$',
                    r'DW_DEV_DW_LOAN_VALUATION')
    , 'uat': (r'COM-DWDBU01.SITUSAMC.COM', r'ODBC Driver 17 for SQL Server', r'python', r'FZ9H2Pcg=z%-cf?$',
              r'DW_UAT_DW_LOAN_VALUATION')
    , 'prod': (
        r'COM-DWAGP01.SITUSAMC.COM', r'SQL Server', r'python', r'FZ9H2Pcg=z%-cf?$', r'DW_PROD_DW_LOAN_VALUATION')
            }

env = env_info['dev']
server = env[0]
driver = env[1]
uname = env[2]
pword = env[3]
dsn = env[4]
dw_db = 'DW_SAMC_INTERNAL'

sql_area_path = r"""
SELECT
	B.AREA_PATH
	,B.SPLIT_VALUE
	,B.SORT_NUMBER
FROM (
SELECT
	A.AREA_PATH
	,SS.VALUE AS SPLIT_VALUE
	,ROW_NUMBER() OVER(PARTITION BY A.AREA_PATH ORDER BY A.AREA_PATH) AS SORT_NUMBER
FROM (
SELECT DISTINCT
	UPPER(TRIM(SystemAreaPath)) AS AREA_PATH
FROM [stage].[SRC_WorkItems] WITH(NOLOCK) 
WHERE
	NULLIF(SystemAreaPath, '') IS NOT NULL
) A
CROSS APPLY STRING_SPLIT(A.AREA_PATH, '\') SS	
) B
ORDER BY
	B.AREA_PATH DESC
	,B.SORT_NUMBER DESC	
"""

conn = pyodbc.connect(
    f"Driver={driver};"
    f"Server={server};"
    f"Database={dw_db};"
    f"UID={uname};"
    f"PWD={pword};"
    , autocommit=True)

area_path_split = pd.read_sql(sql=sql_area_path, con=conn).values.tolist()
area_path_parent = []

# loop excludes last row due to checking next index
for i in range(0, len(area_path_split) - 1):
    if area_path_split[i][2] > area_path_split[i + 1][2]:
        # append child and parent
        area_path_parent.append([area_path_split[i][1], area_path_split[i + 1][1]])
    else:
        # append child and no parent
        area_path_parent.append([area_path_split[i][1], None])

# append last row of area_path_split, it will have no parent based on sort
area_path_parent.append([area_path_split[-1][1], None])

df = pd.DataFrame(data=area_path_parent, columns=['CHILD', 'PARENT'])
print(df)

