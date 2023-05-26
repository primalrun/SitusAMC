import pandas as pd
import pyodbc
import sqlalchemy

server = r'AMC-DLKDWP01.amcfirst.com'
db = r'DW_LOAN_VALUATION'
dsn = r'DW_DEV_DW_LOAN_VALUATION'
target_table = r'SRC_DirtyLoans'

conn01 = pyodbc.connect(
    "Driver={SQL Server};"
    f"Server={server};"
    f"Database={db};"
    "UID=python;"
    "PWD=FZ9H2Pcg=z%-cf?$;"
    , autocommit=True)

sql_select = """
SELECT *
FROM DW_LOAN_VALUATION.stage.SRC_DirtyLoans WITH(NOLOCK)
"""

df_in = pd.read_sql(sql=sql_select, con=conn01)


engine = sqlalchemy.create_engine(
        f"mssql+pyodbc://@{dsn}",
        fast_executemany=True)

conn = engine.connect()

df_in.to_sql(target_table, conn, if_exists='append', index=False)
print('success')