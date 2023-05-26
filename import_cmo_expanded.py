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

input_file = r'C:\Users\jasonwalker\OneDrive - SitusAMC\Documents\project\baseline\stand_alone_file\cmo_expanded\CMO_Expanded.csv'
target_schema = 'dbo'
target_table = 'CMO_EXPANDED'
source_name = 'GINNIE MAE'

conn01 = pyodbc.connect(
    f"Driver={driver};"
    f"Server={server};"
    f"Database={db};"
    f"UID={uname};"
    f"PWD={pword};"
    , autocommit=True)


# import file into dataframe, prepare dataframe for load
df_input = pd.read_csv(filepath_or_buffer=input_file, delimiter=',', keep_default_na=False)
df_input.rename(columns={'CMO CUSIP': 'CUSIP'}, inplace=True)


column_new = [
    'CMO'
    ,'Tranche'
    ,'Bond'
    ,'Website'
    ,'Percent Included'
    ,'As-Of Date'
    ,'CUSIP'
    ,'Pool UPB'
    ,'Remaining Security RPB'
    ,'DATA_SOURCE'
    ,'CREATED_DATE'
    ,'CREATED_USER'
]


df_input['As-Of Date'] = pd.to_datetime(df_input['As-Of Date']).dt.strftime('%Y%m%d')
df_input['Pool UPB'] = df_input['Pool UPB'].astype(str)
df_input['Remaining Security RPB'] = df_input['Remaining Security RPB'].astype(str)
df_input['Pool Interest Rate'] = df_input['Pool Interest Rate'].astype(str)
df_input['RPB Factor'] = df_input['RPB Factor'].astype(str)
df_input['Percent Included'] = df_input['Percent Included'].str.replace('%', '').astype(float) / 100
df_input['Percent Included'] = df_input['Percent Included'].astype(str).replace('1.0', '1')

df_sql = f"""
SELECT SOURCE_SID FROM DW_DIMENSIONS.dbo.MLU_DATA_SOURCE WHERE SOURCE_NAME = '{source_name}'
"""

data_source = pd.read_sql(sql=df_sql, con=conn01).iloc[0, 0]
created_user = pd.read_sql(sql=r'SELECT DATABASE_PRINCIPAL_ID()', con=conn01).iloc[0, 0]
time_stamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
df_input['DATA_SOURCE'] = [data_source for x in range(len(df_input.index))]
df_input['DATA_SOURCE'] = df_input['DATA_SOURCE'].astype(str)
df_input['CREATED_DATE'] = [time_stamp for x in range(len(df_input.index))]
df_input['CREATED_DATE'] = df_input['CREATED_DATE'].astype(str)
df_input['CREATED_USER'] = [created_user for x in range(len(df_input.index))]
df_input['CREATED_USER'] = df_input['CREATED_USER'].astype(str)
df_insert = df_input[column_new]
df_insert = df_insert.rename(columns={
    'Percent Included': 'PERCENT_INCLUDED'
    ,'As-Of Date': 'AS_OF_DATE_KEY'
    ,'Pool UPB': 'POOL_UPB'
    ,'Remaining Security RPB': 'REMAINING_SECURITY_RPB'
})


sql_column = f"""
SELECT
    COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE
    TABLE_SCHEMA = '{target_schema}'
    AND TABLE_NAME = '{target_table}'
ORDER BY ORDINAL_POSITION
"""

# skip first and last 2 columns in target table
target_columns = [x[0] for x in pd.read_sql(sql_column, con=conn01).values.tolist()[1:-2]]
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

    #DELETE DUPLICATES
    duplicate_delete_sql = """
    DELETE FROM dbo.CMO_EXPANDED
    WHERE
        CMO_EXPANDED_KEY IN (
    SELECT
        C2.CMO_EXPANDED_KEY
    FROM (
    SELECT
        C.CMO_EXPANDED_KEY
        ,C.CMO
        ,C.TRANCHE
        ,C.BOND
        ,C.PERCENT_INCLUDED	
        ,C.POOL_UPB
        ,C.REMAINING_SECURITY_RPB
        ,ROW_NUMBER() OVER(
            PARTITION BY C.CMO, C.TRANCHE, C.BOND, C.PERCENT_INCLUDED, C.POOL_UPB, C.REMAINING_SECURITY_RPB
            ORDER BY C.CMO, C.TRANCHE, C.BOND, C.PERCENT_INCLUDED, C.POOL_UPB, C.REMAINING_SECURITY_RPB
            ) AS DELETE_SORT
    FROM dbo.CMO_EXPANDED C WITH(NOLOCK)
    INNER JOIN (
    SELECT
        CMO
        ,TRANCHE
        ,BOND
        ,PERCENT_INCLUDED	
        ,POOL_UPB
        ,REMAINING_SECURITY_RPB
    FROM dbo.CMO_EXPANDED WITH(NOLOCK)
    GROUP BY
        CMO
        ,TRANCHE
        ,BOND
        ,PERCENT_INCLUDED	
        ,POOL_UPB
        ,REMAINING_SECURITY_RPB
    HAVING
        COUNT(*) > 1
    ) C1
        ON C.CMO = C1.CMO
        AND C.TRANCHE = C1.TRANCHE
        AND C.BOND = C1.BOND
        AND C.PERCENT_INCLUDED = C1.PERCENT_INCLUDED
        AND C.POOL_UPB = C1.POOL_UPB
        AND C.REMAINING_SECURITY_RPB = C1.REMAINING_SECURITY_RPB
    ) C2
    WHERE
        C2.DELETE_SORT <> 1
    )
    ;
    """
    cursor.execute(duplicate_delete_sql)
    cursor.commit()


print('SUCCESS')