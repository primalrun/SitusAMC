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

env = env_info['prod']
db = r'DW_LOAN_VALUATION'

server = env[0]
driver = env[1]
uname = env[2]
pword = env[3]
dsn = env[4]

input_file = r'C:\Users\jasonwalker\OneDrive - SitusAMC\Documents\project\baseline\stand_alone_file\pool_level_calc\IMPORT_FILE\POOL_LEVEL_CALCS.xlsx'
target_schema = 'stage'
target_table = 'SRC_POOL_STAT'

conn01 = pyodbc.connect(
    f"Driver={driver};"
    f"Server={server};"
    f"Database={db};"
    f"UID={uname};"
    f"PWD={pword};"
    , autocommit=True)


# import file into dataframe, prepare dataframe for load
column_name_input = [
    'CUSIP'
    ,'POOL_ID'
    ,'POOL_PREFIX'
    ,'ISSUE_DATE'
    ,'ORIGINAL_AGGREGATE_AMOUNT'
    ,'ISSUER'
    ,'POOL_SEASONING'
]

df_input = pd.read_excel(io=input_file, sheet_name='Current Pool Info', header=0, keep_default_na=False, usecols='B,A,C,H,F,BQ,G', names=column_name_input)

column_new = [
    'POOL_ID'
    ,'CUSIP'
    ,'POOL_PREFIX'
    ,'ISSUER'
    ,'ISSUE_DATE'
    ,'POOL_SEASONING'
    ,'ORIGINAL_AGGREGATE_AMOUNT'
]
df_insert = df_input[column_new]

sql_column = f"""
SELECT
    COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE
    TABLE_SCHEMA = '{target_schema}'
    AND TABLE_NAME = '{target_table}'
ORDER BY ORDINAL_POSITION
"""

target_columns = [x[0] for x in pd.read_sql(sql_column, con=conn01).values.tolist()]
insert_column = ', '.join(target_columns)
temp_column = ', '.join([x + ' VARCHAR(500)' for x in target_columns])
value_placeholder = ', '.join('?' for x in target_columns)

temp_table_create_sql = f"""
CREATE TABLE {target_schema}.{target_table}_TEMP (
{temp_column}
);
"""


with conn01.cursor() as cursor:
    # DROP TEMP TABLE
    cursor.execute(f'DROP TABLE IF EXISTS {target_schema}.{target_table}_TEMP')
    cursor.commit()

    # CREATE TEMP TABLE
    cursor.execute(temp_table_create_sql)
    cursor.commit()

    # INSERT TEMP TABLE FROM DATAFRAME
    engine = sqlalchemy.create_engine(
        f"mssql+pyodbc://@{dsn}",
        fast_executemany=True)

    temp_table = f'{target_table}_TEMP'
    df_insert.to_sql(name=temp_table, con=engine, schema=f'{target_schema}', if_exists='append', index=False)

    # TRUNCATE TARGET TABLE
    cursor.execute(f'TRUNCATE TABLE {target_schema}.{target_table}')
    cursor.commit()

    #INSERT INTO TARGET TABLE FROM TEMP TABLE
    insert_sql = f"""
    INSERT INTO {target_schema}.{target_table} (
    {insert_column}
    )
    SELECT
        {insert_column}
    FROM {target_schema}.{target_table}_TEMP
    """

    cursor.execute(insert_sql)
    cursor.commit()

    #DROP TEMP TABLE
    cursor.execute(f'DROP TABLE IF EXISTS {target_schema}.{target_table}_TEMP')
    cursor.commit()

print('SUCCESS')