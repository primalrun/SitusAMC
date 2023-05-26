import pyodbc
import pandas as pd

server = r'COM-DLKDWD01.SITUSAMC.COM'
db = r'DW_DIMENSIONS'

conn01 = pyodbc.connect(
    "Driver={ODBC Driver 17 for SQL Server};"
    f"Server={server};"
    f"Database={db};"
    "UID=python;"
    "PWD=BPcpfhSx0t2669St2jDX;"
    , autocommit=True)

# conn01 = pyodbc.connect(
#     "Driver={ODBC Driver 17 for SQL Server};"
#     f"Server={server};"
#     f"Database={db};"
#     "Trusted_Connection=yes;"
#     , autocommit=True)


sql_select = """
SELECT DayName
FROM DW_DIMENSIONS.dbo.MLU_DATE
"""

df = pd.read_sql(sql=sql_select, con=conn01)
unique_name = df['DayName'].unique()
df_result = pd.DataFrame(data=unique_name, columns=['DAY_NAME'])

print(df_result)
