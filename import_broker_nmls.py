import pandas as pd
import pyodbc
import datetime
import sqlalchemy


# environment: (server, driver, uname, pword, dsn)
env_info = {'dev': (r'COM-DLKDWD01.SITUSAMC.COM', r'ODBC Driver 17 for SQL Server', r'python', r'FZ9H2Pcg=z%-cf?$',
                    r'DW_DEV_DW_LOAN_VALUATION')
    , 'uat': (r'COM-DWDBU01.SITUSAMC.COM', r'ODBC Driver 17 for SQL Server', r'python', r'FZ9H2Pcg=z%-cf?$',
              r'DW_UAT_DW_LOAN_VALUATION')
    , 'prod': (r'COM-DWAGP01.SITUSAMC.COM', r'SQL Server', r'python', r'FZ9H2Pcg=z%-cf?$', r'DW_PROD_DW_LOAN_VALUATION')
            }

env = env_info['prod']
db = r'DW_LOAN_VALUATION'

server = env[0]
driver = env[1]
uname = env[2]
pword = env[3]
dsn = env[4]

input_file = r'C:\Users\jasonwalker\OneDrive - SitusAMC\Documents\project\baseline\stand_alone_file\nmls\IMPORT\NMLS.xlsx'
target_schema = 'stage'
target_table = 'SRC_BROKER_NMLS'
source_name = 'GINNIE MAE'

conn01 = pyodbc.connect(
    f"Driver={driver};"
    f"Server={server};"
    f"Database={db};"
    f"UID={uname};"
    f"PWD={pword};"
    , autocommit=True)


# function
def insert_data(schema
                , table
                , skip_column_position=[]  # list of column positions to skip starting at position 1
                ):
    if len(skip_column_position) > 0:
        skip_column_string = 'AND ORDINAL_POSITION NOT IN (' + ', '.join(str(s) for s in skip_column_position) + ')'
    else:
        skip_column_string = ''

    sql_column = f"""
    SELECT
        COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE
        TABLE_SCHEMA = '{target_schema}'
        AND TABLE_NAME = '{target_table}'
        {skip_column_string}
    ORDER BY ORDINAL_POSITION
    """

    target_columns = [x[0] for x in pd.read_sql(sql_column, con=conn01).values.tolist()]
    insert_column = ', '.join(target_columns)
    temp_column = ', '.join([x + ' VARCHAR(500)' for x in target_columns])
    value_placeholder = ', '.join('?' for x in target_columns)

    return insert_column, temp_column, value_placeholder


# import file into dataframe, prepare dataframe for load, insert data in database
column_name_input = [
    'NMLS_ID'
    , 'ADDRESS'
    , 'BROKER_NAME'
    , 'STATE_CODE'
    , 'ZIP_CODE'
    ]

df_input = pd.read_excel(
    io=input_file
    , sheet_name=r'Sheet1'
    , header=0
    , keep_default_na=False
    , names=column_name_input
)


# remove duplicate rows based on BROKER_NAME
df_input.drop_duplicates(subset=['BROKER_NAME'], keep='first', inplace=True)

df_input['NMLS_ID'] = df_input['NMLS_ID'].astype(str)
df_input['ADDRESS'] = df_input['ADDRESS'].astype(str)
df_input['BROKER_NAME'] = df_input['BROKER_NAME'].astype(str)
df_input['STATE_CODE'] = df_input['STATE_CODE'].astype(str)
df_input['ZIP_CODE'] = df_input['ZIP_CODE'].astype(str)

df_input['ADDRESS'] = df_input['ADDRESS'].map(lambda x: x.strip().upper())
df_input['BROKER_NAME'] = df_input['BROKER_NAME'].map(lambda x: x.strip().upper())

sql_data_source = f"""
SELECT SOURCE_SID FROM DW_DIMENSIONS.dbo.MLU_DATA_SOURCE WHERE SOURCE_NAME = '{source_name}'
"""
data_source = pd.read_sql(sql=sql_data_source, con=conn01).iloc[0, 0]
created_user = pd.read_sql(sql=r'SELECT DATABASE_PRINCIPAL_ID()', con=conn01).iloc[0, 0]
time_stamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
df_input['DATA_SOURCE'] = [data_source for x in range(len(df_input.index))]
df_input['DATA_SOURCE'] = df_input['DATA_SOURCE'].astype(str)
df_input['CREATED_DATE'] = [time_stamp for x in range(len(df_input.index))]
df_input['CREATED_DATE'] = df_input['CREATED_DATE'].astype(str)
df_input['CREATED_USER'] = [created_user for x in range(len(df_input.index))]
df_input['CREATED_USER'] = df_input['CREATED_USER'].astype(str)

insert_data_return = insert_data(schema=target_schema, table=target_table, skip_column_position=[1, 10, 11])
insert_column = insert_data_return[0]
temp_column = insert_data_return[1]
insert_value = insert_data_return[2]

with conn01.cursor() as cursor:
    # drop temp table
    cursor.execute(f'DROP TABLE IF EXISTS {target_schema}.{target_table}_TEMP')

    # create temp table
    temp_table_create_sql = f"""
    CREATE TABLE {target_schema}.{target_table}_TEMP (
    {temp_column}
    );
    """

    cursor.execute(temp_table_create_sql)

    # insert temp table from dataframe
    engine = sqlalchemy.create_engine(
        f"mssql+pyodbc://@{dsn}",
        fast_executemany=True)

    temp_table = f'{target_table}_TEMP'
    df_input.to_sql(name=temp_table, con=engine, schema=f'{target_schema}', if_exists='append', index=False)

    # truncate stage table
    cursor.execute(f'TRUNCATE TABLE {target_schema}.{target_table};')

    #insert into stage table from temp table
    insert_sql = f"""
    INSERT INTO {target_schema}.{target_table} (
    {insert_column}
    )
    SELECT
        {insert_column}
    FROM {target_schema}.{target_table}_TEMP
    """

    cursor.execute(insert_sql)

    #drop temp table
    cursor.execute(f'DROP TABLE IF EXISTS {target_schema}.{target_table}_TEMP')



print('success')
