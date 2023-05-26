import common_functions as cfx
import os
import sys

source_db = r'AzureDevOps'
source_schema = 'dbo'
source_object = 'WorkLogs'
out_file_dir = r'c:\temp'

server, driver, uname, pword, dsn = cfx.environment_variables('data_lake')

server_class = cfx.ConnectSQLServer(server=server
                                    , driver=driver
                                    , uname=uname
                                    , pword=pword)

out_file = os.path.join(out_file_dir, f'{source_schema}_{source_object}_NOT_NULL_COLUMN.txt')

if os.path.exists(out_file):
    os.remove(out_file)

with server_class.connection(authentication='windows', db=source_db) as conn:
    column_not_null_sql = cfx.table_column_null_check_sql(p_schema=source_schema, p_object=source_object, p_con=conn)

with open(out_file, 'w') as w:
    w.write(column_not_null_sql)

print('Success, file created, and file opened')
os.startfile(out_file)


