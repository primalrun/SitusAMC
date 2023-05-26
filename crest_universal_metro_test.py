import pandas as pd
import os
import pyodbc
import common_functions as cfx
import numpy as np

source_db = r'DW_MARKETS'



server, driver, uname, pword, dsn = cfx.environment_variables('dev')


rent_growth_year_change = 1


# function
def fx_df_column_null_check(df):
	column_is_only_null = []
	for column in df:
		if df[column].isnull().sum() == len(df[column].index):
			column_is_only_null.append(True)
		else:
			column_is_only_null.append(False)
	return column_is_only_null


rent_growth_score = f"RENT_GROWTH_SCORE_{rent_growth_year_change}_YEAR"


sql = f"""
SELECT
	F.FACT_CREST_UNIVERSAL_METROPOLITAN_KEY
	,{rent_growth_score} AS RENT_GROWTH_SCORE
FROM [dbo].[vw_FACT_CREST_UNIVERSAL_METROPOLITAN] F WITH(NOLOCK)
WHERE 
	FILE_DATE_YEAR = 2022
	AND FILE_DATE_QUARTER = 2
	AND TIER = 'TERTIARY'
	AND PROPERTY_TYPE = 'WAREHOUSE'
	AND REGION = 'MIDWEST'
"""

server_class = cfx.ConnectSQLServer(server=server
                                    , driver=driver
                                    , uname=uname
                                    , pword=pword)

with server_class.connection(authentication='sql server', db=source_db) as conn:
    data_df = pd.read_sql(sql=sql, con=conn)

column_null_check = fx_df_column_null_check(data_df)


for x in range(0, len(data_df.columns)):
	if column_null_check[x] is True:
		data_df[data_df.columns[x]] = 'NULL'


out_file = r'c:\temp\t1.csv'
data_df.to_csv(out_file, index=False)
os.startfile(out_file)

