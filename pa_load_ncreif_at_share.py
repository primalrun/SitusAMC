import common_functions as cfx
import pandas as pd
import openpyxl
import sys
import sqlalchemy
import os

target_db = r'DW_MARKETS'
target_schema = 'stage'
ncreif_source_file = r'C:\Users\jasonwalker\OneDrive - SitusAMC\Documents\project\PERFORMANCE_ATTRIBUTION\2022Q4\SOURCE_FILES\NCREIF Data at Share 4Q22.xlsx'
quarter_search = '4Q22 '

server, driver, uname, pword, dsn = cfx.environment_variables('dev')


ncreif_subject = {
    'PropType at Share': 'IMPORT_NCREIF_PROPTYPE_ATSHARE'
    , 'SubType at Share': 'IMPORT_NCREIF_SUBTYPE_ATSHARE'
    , 'Region at Share': 'IMPORT_NCREIF_REGION_ATSHARE'
    , 'Division at Share': 'IMPORT_NCREIF_DIVISION_ATSHARE'
    , 'CBSA or Div at Share': 'IMPORT_NCREIF_CBSADIV_ATSHARE'
    , 'State at Share': 'IMPORT_NCREIF_STATE_ATSHARE'
}

column_rename = {
    'Income Return': 'INCOME_RETURN'
    , 'Capital Return':  'CAPITAL_RETURN'
    , 'Total Return':  'TOTAL_RETURN'
    , 'Prop Count':  'PROP_COUNT'
}

target_column_exclude = [
    'STAGE_KEY'
    , 'DATA_SOURCE'
    , 'CREATED_DATE'
    , 'CREATED_USER'
    , 'MODIFIED_DATE'
    , 'MODIFIED_USER'
]


sheet_search = [quarter_search + s for s in ncreif_subject]

# ncreif data
wb = openpyxl.load_workbook(filename=ncreif_source_file, read_only=True, data_only=True)
sheets = [sheet for sheet in wb.sheetnames if sheet in sheet_search]

if len(sheets) < len(sheet_search):
    print('missing sheet(s), verify sheet name in source file')
    sys.exit()

server_class = cfx.ConnectSQLServer(server=server
                                    , driver=driver
                                    , uname=uname
                                    , pword=pword)

with server_class.connection(authentication='windows', db=target_db) as conn:
    cursor = conn.cursor()

    engine = sqlalchemy.create_engine(
        f"mssql+pyodbc://@{dsn}",
        fast_executemany=True)

    # loop through each sheet
    for sheet in sheets:
        subject = sheet.replace(quarter_search, '')
        table_iter = ncreif_subject[subject]
        target_table = table_iter.replace('IMPORT', 'SRC')
        get_column_sql = cfx.table_columns_sql(target_schema, target_table)
        target_table_columns = pd.read_sql(sql=get_column_sql, con=conn).values.tolist()
        target_table_columns = [''.join(ele) for ele in target_table_columns]
        import_table_columns = [
            col + ' VARCHAR(MAX)' for col in target_table_columns if col not in target_column_exclude]
        import_table_column_string = '\n, '.join(import_table_columns)
        df = pd.read_excel(io=ncreif_source_file, sheet_name=sheet, dtype=str)
        df.rename(columns=column_rename, inplace=True)
        cursor.execute(f'DROP TABLE IF EXISTS {target_schema}.{table_iter}')
        table_iter_create_sql = f"""CREATE TABLE {target_schema}.{table_iter} (
        {import_table_column_string}
        )
        """
        cursor.execute(table_iter_create_sql)
        df.to_sql(name=table_iter, con=engine, schema=f'{target_db}.{target_schema}', if_exists='append', index=False)





print('success, process complete')





