import common_functions as cfx
import pandas as pd
import os
import sys

source_db = r'AzureDevOps'
source_schema = ['dbo']
source_object_type = ['table']
target_db = r'DW_SAMC_INTERNAL'
target_schema = 'stage'
out_dir = r'c:\temp\DDL_SCRIPT'

server, driver, uname, pword, dsn = cfx.environment_variables('data_lake')

server_class = cfx.ConnectSQLServer(server=server
                                    , driver=driver
                                    , uname=uname
                                    , pword=pword)

source_objects = []

with server_class.connection(authentication='windows', db=source_db) as conn:
    # loop through schema and object type
    for schema_iter in source_schema:
        for object_type_iter in source_object_type:
            df_iter = pd.read_sql(
                sql=cfx.schema_objects(schema=schema_iter, object_type=object_type_iter), con=conn)
            for x in df_iter['OBJECT_NAME'].values.tolist():
                source_objects.append([schema_iter, x])

# loop through source objects and build deployment scripts
for obj in source_objects:
    schema_iter = obj[0]
    object_iter = obj[1]

    # get column data
    df_iter = pd.read_sql(
        sql=cfx.table_column_attributes(schema=schema_iter, table=object_iter), con=conn)

    df_iter['COLUMN_DDL'] = df_iter.apply(lambda fx: cfx.column_data_type(
        fx['COLUMN_NAME']
        , fx['DATA_TYPE']
        , fx['MAX_LENGTH']
        , fx['PRECISION']
        , fx['SCALE']
        , fx['IS_NULLABLE']
    )
                                          , axis=1)

    df_iter['COLUMN'] = df_iter.apply(lambda fx: cfx.quote_string(fx['COLUMN_NAME'], '['), axis=1)
    df_column = df_iter[['COLUMN', 'COLUMN_DDL']].copy()
    column_ddl = df_column['COLUMN_DDL'].values.tolist()
    column_name = df_column['COLUMN'].values.tolist()

    # test
    # print(object_iter)
    # print(column_ddl)
    # print(df_iter)



    # source view
    source_view_file = os.path.join(out_dir, f'{target_schema}^vw_SRC_{object_iter}.sql')
    source_view_sql = cfx.source_view_script(source_server=server
                                             , source_db=source_db
                                             , source_schema=schema_iter
                                             , source_object=object_iter
                                             , target_db=target_db
                                             , target_schema=target_schema
                                             , column_name=column_name
                                             )
    with open(source_view_file, 'w') as w:
        w.write(source_view_sql)


    # source stage table
    source_stage_table_file = os.path.join(out_dir, f'{target_schema}^SRC_{object_iter}.sql')
    source_stage_table_sql = cfx.source_stage_table_script(source_object=object_iter
                                                            , target_db=target_db
                                                            , target_schema=target_schema
                                                            , column_ddl=column_ddl)
    with open(source_stage_table_file, 'w') as w:
        w.write(source_stage_table_sql)


print('Success, files are created, and file directory is opened')
os.startfile(out_dir)
