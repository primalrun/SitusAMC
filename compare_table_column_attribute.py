import functions as fx
import os
import pyodbc
import pandas as pd

source_server_login = dict(auth=r'sql_server',
                           server=r'ULTIDB',
                           database=r'ULTIPRO_HQM',
                           username=r'VPNSQL2_SPS',
                           password=r'Ca!atre5AwrE')

destination_server_login = dict(auth=r'windows',
                                server=r'ULTIDB',
                                database=r'ULTIPRO_HQM',
                                username=None,
                                password=None)

table = 'ipDedHist'
schema = 'dbo'

with fx.connect_sql_server(login_dict=source_server_login) as connection_sql_server:
    sql = (r'SELECT TableName = T.name'
           r',SchemaName = S.name'
           r',C.column_id'
           r',ColumnName = c.name'
           r',DataType = TP.name'
           r',C.max_length'
           r',C.precision'
           r',c.scale'
           r',c.is_nullable'
           r'FROM SYS.tables T'
           r'INNER JOIN SYS.schemas S ON T.schema_id = S.schema_id'
           r'INNER JOIN SYS.columns C ON T.object_id = C.object_id'
           r'INNER JOIN SYS.types TP ON C.user_type_id = TP.user_type_id'
           f'WHERE T.name = {table}'
           f'    AND S.name = {schema}'
           r'ORDER BY C.column_id'
           )
    data = pd.read_sql(sql, connection_sql_server)
    source_data = pd.DataFrame(data)

print(source_data)
