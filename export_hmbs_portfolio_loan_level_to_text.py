import pandas as pd
import pyodbc

# environment: (server, driver, uname, pword, dsn)
env_info = {'dev': (r'COM-DLKDWD01.SITUSAMC.COM', r'ODBC Driver 17 for SQL Server', r'python', r'FZ9H2Pcg=z%-cf?$', r'DW_DEV_DW_LOAN_VALUATION')
    , 'uat': (r'COM-DWDBU01.SITUSAMC.COM', r'ODBC Driver 17 for SQL Server', r'python' ,r'FZ9H2Pcg=z%-cf?$', r'DW_UAT_DW_LOAN_VALUATION')
    , 'prod': (r'COM-DWAGP01.SITUSAMC.COM', r'SQL Server', r'python', r'FZ9H2Pcg=z%-cf?$', r'DW_PROD_DW_LOAN_VALUATION')
     }

env = env_info['dev']
db = r'DW_LOAN_VALUATION'
out_file = r'c:\temp\HMBS_PORTFOLIO_LOAN_LEVEL.TXT'

server = env[0]
driver = env[1]
uname = env[2]
pword = env[3]
dsn = env[4]

conn01 = pyodbc.connect(
    f"Driver={driver};"
    f"Server={server};"
    f"Database={db};"
    f"UID={uname};"
    f"PWD={pword};"
    , autocommit=True)

sql_select = """
SELECT
[POOL_ID]
,[DISCLOSURE_SEQUENCE_NUMBER]
,[AS_OF_DATE_KEY]
,[SEQUENCE_NUMBER_SUFFIX]
,[HECM_LOAN_CURRENT_INTEREST_RATE]
,[CURRENT_HECM_LOAN_BALANCE]
,[PARTICIPATION_UPB]
,[PARTICIPATION_INTEREST_RATE]
,[REMAINING_AVAILABLE_LINE_OF_CREDIT_AMOUNT]
,[MIP_BASIS_POINTS]
,[MONTHLY_SERVICING_FEE_AMOUNT]
FROM [stage].[HMBS_PORTFOLIO_LOAN_LEVEL] WITH(NOLOCK)
WHERE
	AS_OF_DATE_KEY = 20211101
"""

df = pd.read_sql(sql=sql_select, con=conn01)
df.to_csv(path_or_buf=out_file, index=False)
print('SUCCESS')

