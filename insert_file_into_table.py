import os
from configparser import ConfigParser
import functions as fx
import pandas as pd
import datetime
import sys
import os

# parameter

# config.ini shows possible values
server_parm = 'DW_PROD'

default_db = 'DW_DIMENSIONS'
server_auth = r'windows'
source_file = r'c:\temp\STATE_TYPE.txt'

# read config.ini file
config_object = ConfigParser()
config_object.read('config.ini')

server_config = config_object[server_parm]
server_name = server_config['server_name']

server_login = dict(auth=server_auth,
                    server=server_name,
                    database=default_db,
                    username=None,
                    password=None)

with fx.connect_sql_server(login_dict=server_login) as connection_sql_server:
    df_column = ['STATE_TYPE_CODE', 'STATE_TYPE_NAME']
    df_source = pd.read_csv(source_file, sep='\t', keep_default_na=False)

    user_id = pd.read_sql('SELECT DATABASE_PRINCIPAL_ID()', con=connection_sql_server).values.tolist()[0][0]
    datestamp = datetime.datetime.now()

    cursor = connection_sql_server.cursor()
    df_source.columns = df_column
    df_source['CREATED_DATE'] = [datestamp for x in range(0, len(df_source.index))]
    df_source['CREATED_USER'] = [user_id for x in range(0, len(df_source.index))]

    # df_source.to_excel(r'c:\temp\insert.xlsx')
    # os.startfile(r'c:\temp\insert.xlsx')

    sql_insert = """
    INSERT INTO DW_DIMENSIONS.[dbo].[MLU_STATE_TYPE]
       ([STATE_TYPE_CODE]
       ,[STATE_TYPE_NAME]
       ,[CREATED_DATE]
       ,[CREATED_USER])
     VALUES  (?, ?, ?, ?)
    """

    for index, row in df_source.iterrows():
        cursor.execute(sql_insert, row.STATE_TYPE_CODE, row.STATE_TYPE_NAME, row.CREATED_DATE, row.CREATED_USER)

    cursor.close()

print('SUCCESS')
