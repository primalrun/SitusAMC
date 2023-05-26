import pyodbc
import pandas as pd

server = r'COM-DLKAGP01.SITUSAMC.COM'
db = r'BaselineV2'

conn01 = pyodbc.connect(
    "Driver={ODBC Driver 17 for SQL Server};"
    f"Server={server};"
    f"Database={db};"
    r'Trusted_Connection=yes'
    , autocommit=True)


# conn01 = pyodbc.connect(
#     "Driver={ODBC Driver 17 for SQL Server};"
#     f"Server={server};"
#     f"Database={db};"
#     "UID=python;"
#     "PWD=FZ9H2Pcg=z%-cf?$;"
#     , autocommit=True)

sql_select = """
SELECT TOP 10 *
FROM BaselineV2.dbo.GinnieMaeHMBSLoanLevelFixedAndAnnualAdjustable
"""




df = pd.read_sql(sql=sql_select, con=conn01)


print(df)
