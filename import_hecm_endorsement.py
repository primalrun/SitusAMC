import pandas as pd
import numpy as np
import pyodbc
import datetime
import sqlalchemy
import sys

# environment: (server, driver, uname, pword, dsn)
env_info = {'dev': (r'COM-DLKDWD01.SITUSAMC.COM', r'ODBC Driver 17 for SQL Server', r'python', r'FZ9H2Pcg=z%-cf?$',
                    r'DW_DEV_DW_LOAN_VALUATION')
    , 'uat': (r'COM-DWDBU01.SITUSAMC.COM', r'ODBC Driver 17 for SQL Server', r'python', r'FZ9H2Pcg=z%-cf?$',
              r'DW_UAT_DW_LOAN_VALUATION')
    , 'prod': (r'COM-DWAGP01.SITUSAMC.COM', r'SQL Server', r'python', r'FZ9H2Pcg=z%-cf?$', r'DW_PROD_DW_LOAN_VALUATION')
            }

env = env_info['dev']
db = r'DW_LOAN_VALUATION'

server = env[0]
driver = env[1]
uname = env[2]
pword = env[3]
dsn = env[4]

input_file = r'C:\Users\jasonwalker\OneDrive - SitusAMC\Documents\project\baseline\stand_alone_file\hecm_endorsement\IMPORT\HECM_ENDORSEMENT.txt'
target_schema = 'stage'
target_table = 'SRC_HECM_ENDORSEMENT'
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

def string_to_int(value):
    if len(value) == 0:
        return None
    else:
        return int(float(value))

def string_to_float(value):
    if len(value) == 0:
        return None
    else:
        return float(value)



# import file into dataframe, prepare dataframe for load, insert data in database
column_name_input = [
    'PROPERTY_STATE_CODE'
    , 'PROPERTY_COUNTY'
    , 'PROPERTY_CITY'
    , 'PROPERTY_ZIP_CODE'
    , 'ORIGINATING_MORTGAGEE_SPONSOR_ORIGINATOR'
    , 'ORIGINATING_MORTGAGEE_NUMBER'
    , 'SPONSOR_NAME'
    , 'SPONSOR_NUMBER'
    , 'SPONSOR_ORIGINATOR'
    , 'CURRENT_SERVICER_ID'
    , 'PREVIOUS_SERVICER'
    , 'NMLS'
    , 'STANDARD_SAVER'
    , 'PURCHASE_REFINANCE'
    , 'HECM_TYPE'
    , 'RATE_TYPE'
    , 'INTEREST_RATE'
    , 'INITIAL_PRINCIPAL_LIMIT'
    , 'MAXIMUM_CLAIM_AMOUNT'
    , 'ENDORSEMENT_YEAR'
    , 'ENDORSEMENT_MONTH'
]
df_input = pd.read_csv(filepath_or_buffer=input_file
                       , delimiter=','
                       , keep_default_na=False
                       , encoding='ISO-8859-1'
                       , names=column_name_input
                       , header=0
                       , dtype=str
                       )


interest = set(df_input['INTEREST_RATE'].unique().tolist())


with open(r'c:\temp\test.csv', 'w') as fw:
    for x in interest:
        fw.write(str(x) + '\n')


sys.exit()



df_input['ENDORSEMENT_YEAR'] = df_input['ENDORSEMENT_YEAR'].map(lambda x: string_to_int(x))
df_input['ENDORSEMENT_MONTH'] = df_input['ENDORSEMENT_MONTH'].map(lambda x: string_to_int(x))
df_input['PROPERTY_ZIP_CODE'] = df_input['PROPERTY_ZIP_CODE'].map(lambda x: string_to_int(x))
df_input['CURRENT_SERVICER_ID'] = df_input['CURRENT_SERVICER_ID'].map(lambda x: string_to_int(x))
df_input['PREVIOUS_SERVICER'] = df_input['PREVIOUS_SERVICER'].map(lambda x: string_to_int(x))
df_input['INTEREST_RATE'] = df_input['INTEREST_RATE'].map(lambda x: string_to_float(x))
df_input['INITIAL_PRINCIPAL_LIMIT'] = df_input['INITIAL_PRINCIPAL_LIMIT'].map(lambda x: string_to_float(x))
df_input['MAXIMUM_CLAIM_AMOUNT'] = df_input['MAXIMUM_CLAIM_AMOUNT'].map(lambda x: string_to_float(x))



df_input['ENDORSEMENT_YEAR'] = df_input['ENDORSEMENT_YEAR'].fillna(0)
df_input['ENDORSEMENT_MONTH'] = df_input['ENDORSEMENT_MONTH'].fillna(0)
df_input['PROPERTY_ZIP_CODE'] = df_input['PROPERTY_ZIP_CODE'].fillna(0)
df_input['CURRENT_SERVICER_ID'] = df_input['CURRENT_SERVICER_ID'].fillna(0)
df_input['PREVIOUS_SERVICER'] = df_input['PREVIOUS_SERVICER'].fillna(0)


df_input['ENDORSEMENT_YEAR'] = df_input['ENDORSEMENT_YEAR'].astype(np.int64)
df_input['ENDORSEMENT_MONTH'] = df_input['ENDORSEMENT_MONTH'].astype(np.int64)
df_input['PROPERTY_ZIP_CODE'] = df_input['PROPERTY_ZIP_CODE'].astype(np.int64)
df_input['CURRENT_SERVICER_ID'] = df_input['CURRENT_SERVICER_ID'].astype(np.int64)
df_input['PREVIOUS_SERVICER'] = df_input['PREVIOUS_SERVICER'].astype(np.int64)


df_input['ORIGINATING_MORTGAGEE_NUMBER'] = df_input['ORIGINATING_MORTGAGEE_NUMBER'].astype(str)
df_input['SPONSOR_NUMBER'] = df_input['SPONSOR_NUMBER'].astype(str)
df_input['CURRENT_SERVICER_ID'] = df_input['CURRENT_SERVICER_ID'].astype(str)
df_input['NMLS'] = df_input['NMLS'].astype(str)

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

insert_data_return = insert_data(schema=target_schema, table=target_table, skip_column_position=[1, 26, 27])
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
