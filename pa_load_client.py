import common_functions as cfx
import pandas as pd
import openpyxl
import sys
import sqlalchemy
import os

server, driver, uname, pword, dsn = cfx.environment_variables('prod')
client_name = 'HINES'
target_db = r'DW_PERFORMANCE_ATTRIBUTION'
target_schema = 'stage'
temp_table = 'IMPORT_CLIENT_PERFORMANCE_ATTRIBUTION'
target_table = 'SRC_CLIENT_PERFORMANCE_ATTRIBUTION'
client_source_file = r'C:\Users\jasonwalker\OneDrive - SitusAMC\Documents\project\PERFORMANCE_ATTRIBUTION\2023Q1\Hines Data as of 2023q1.xlsx'
source_sheet_name = r'2023q1'

target_column_exclude = [
    'STAGE_KEY'
    , 'DATA_SOURCE'
    , 'CREATED_DATE'
    , 'CREATED_USER'
    , 'MODIFIED_DATE'
    , 'MODIFIED_USER'
]



# client data
wb = openpyxl.load_workbook(filename=client_source_file, read_only=True, data_only=True)

server_class = cfx.ConnectSQLServer(server=server
                                    , driver=driver
                                    , uname=uname
                                    , pword=pword)

with server_class.connection(authentication='windows', db=target_db) as conn:
    cursor = conn.cursor()

    engine = sqlalchemy.create_engine(
        f"mssql+pyodbc://@{dsn}",
        fast_executemany=True)

    table_iter = temp_table
    get_column_sql = cfx.table_columns_sql(target_schema, target_table)
    target_table_columns = pd.read_sql(sql=get_column_sql, con=conn).values.tolist()
    target_table_columns = [''.join(ele) for ele in target_table_columns]
    import_table_columns = [
        col + ' VARCHAR(MAX)' for col in target_table_columns if col not in target_column_exclude]
    import_table_column_string = '\n, '.join(import_table_columns)
    df = pd.read_excel(io=client_source_file, sheet_name=source_sheet_name, dtype=str)

    print(df['AcquisitionDate'].head())
    sys.exit()

    # make column names uppercase
    df.columns = [c.upper() for c in df.columns]

    # columns to exclude in load
    source_column_exclude = [col for col in df.columns if col not in target_table_columns]

    # remove columns not needed
    df.drop(source_column_exclude, axis=1, inplace=True)

    # add client name
    client_name_data = [client_name for x in range(0, len(df.index))]
    df.insert(0, 'CLIENTNAME', client_name_data)

    cursor.execute(f'DROP TABLE IF EXISTS {target_schema}.{table_iter}')
    table_iter_create_sql = f"""CREATE TABLE {target_schema}.{table_iter} (
    {import_table_column_string}
    )
    """
    cursor.execute(table_iter_create_sql)
    df.to_sql(name=table_iter, con=engine, schema=f'{target_db}.{target_schema}', if_exists='append', index=False)

print('success, process complete')





