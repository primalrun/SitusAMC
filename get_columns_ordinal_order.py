import os
from configparser import ConfigParser
import functions as fx
import pandas as pd

# parameter

# config.ini shows possible values
server_parm = 'DW_PROD'

default_db = 'DW_PROPERTY_VALUATION'
server_auth = r'windows'
schema = r'stage'
table = r'SRC_DEMO_INTERNATIONAL_FACTS'
out_file = r'c:\temp\columns_ordinal_sort.txt'

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
    sql = (
        r"SELECT COLUMN_NAME "
        r"FROM INFORMATION_SCHEMA.COLUMNS "
        f"WHERE TABLE_SCHEMA = '{schema}' "
        f"	AND TABLE_NAME = '{table}' "
        r"ORDER BY ORDINAL_POSITION"
    )

    data = pd.read_sql(sql, connection_sql_server)
    df = pd.DataFrame(data)
    data = df['COLUMN_NAME'].values.tolist()

with open(out_file, 'w') as f:
    for count, value in enumerate(data):
        if count == 0:
            f.write(value + '\n')
        else:
            f.write(',' + value + '\n')

os.startfile(out_file)
