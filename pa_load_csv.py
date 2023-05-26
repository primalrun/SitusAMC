import pandas as pd
import pyodbc
import datetime
import sqlalchemy
import sys

# environment: (server, driver, uname, pword, dsn)
env_info = {'dev': (r'COM-DLKDWD01.SITUSAMC.COM', r'ODBC Driver 17 for SQL Server', r'python', r'FZ9H2Pcg=z%-cf?$', r'DW_DEV_DW_LOAN_VALUATION')
    , 'uat': (r'COM-DWDBU01.SITUSAMC.COM', r'ODBC Driver 17 for SQL Server', r'python' ,r'FZ9H2Pcg=z%-cf?$', r'DW_UAT_DW_LOAN_VALUATION')
    , 'prod': (r'COM-DWAGP01.SITUSAMC.COM', r'SQL Server', r'python', r'FZ9H2Pcg=z%-cf?$', r'DW_PROD_DW_LOAN_VALUATION')
     }

env = env_info['uat']
db = r'DW_MARKETS'

server = env[0]
driver = env[1]
uname = env[2]
pword = env[3]
dsn = env[4]

input_file = r'C:\Users\jasonwalker\OneDrive - SitusAMC\Documents\project\PERFORMANCE_ATTRIBUTION\FILES\2022Q4\2022Q4PROPTYPE_ATSHARE.csv'
target_schema = 'stage'
target_table = 'SRC_NCREIF_PROPTYPE_ATSHARE_IMPORT'

conn01 = pyodbc.connect(
    f"Driver={driver};"
    f"Server={server};"
    f"Database={db};"
    f"UID={uname};"
    f"PWD={pword};"
    , autocommit=True)


# import file into dataframe, prepare dataframe for load
df_input = pd.read_csv(filepath_or_buffer=input_file, delimiter=',', keep_default_na=False)


with conn01.cursor() as cursor:
    engine = sqlalchemy.create_engine(
        f"mssql+pyodbc://@{dsn}",
        fast_executemany=True)

    df_input.to_sql(name=target_table, con=engine, schema=f'{target_schema}', if_exists='append', index=False)


print('success')

