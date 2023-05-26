import pandas as pd
import os
import pyodbc
import common_functions as cfx
import numpy as np
import sys

source_db = r'DW_DIMENSIONS'

server, driver, uname, pword, dsn = cfx.environment_variables('dev')


# function
def fx_metro_clean(metro):
    metro = str(metro)
    if len(metro) < 5:
        return metro
    if metro[-3] == ' ':
        state_code = metro[-2:].strip()
        city_group = metro[:-4].strip()
        city_group = city_group.replace(' - ', '-')
        if r'/' in city_group:
            # cities are currently separated by '/'
            city_group = city_group
        else:
            # change string so cities are separated by '/'
            city_group = city_group.replace('-', r'/')
            city_group = city_group.replace(' - ', r'/')
        return city_group + ' - ' + state_code
    else:
        return metro

def fx_is_nan(value):
    return value != value


def fx_metro_new(single_city_mlu
                 , metro_clean):
    if fx_is_nan(single_city_mlu) is True:
        return metro_clean
    else:
        return single_city_mlu

def fx_state_code(metro):
    metro = str(metro)
    if len(metro) < 5:
        return 'NA'
    if metro[-3] == ' ':
        return metro[-2:].strip()
    else:
        return 'NA'


sql_source_metro = f"""
DECLARE @ALIAS_TYPE VARCHAR(100) = 'METROPOLITAN';
DECLARE @DIM_NULL_KEY BIGINT = (SELECT METROPOLITAN_KEY FROM [dbo].[MLU_METROPOLITAN] WHERE METROPOLITAN_NAME = 'UNKNOWN');
DECLARE @DATA_SOURCE BIGINT = 130047

SELECT
	UPPER(TRIM(S.Metro)) AS METROPOLITAN_SOURCE
FROM [stage].[SRC_METROPOLITAN] S WITH(NOLOCK)
LEFT JOIN [dbo].[REF_ALIAS] RA WITH(NOLOCK)
	ON RA.ALIAS_TYPE = @ALIAS_TYPE
	AND RA.DATA_SOURCE = @DATA_SOURCE
	AND UPPER(TRIM(S.Metro)) = RA.ALIAS
WHERE
	NULLIF(S.Metro, '') IS NOT NULL
	AND COALESCE(RA.DIM_KEY, @DIM_NULL_KEY) = @DIM_NULL_KEY
"""

sql_metro_one_city_one_state = f"""
SELECT
	S.CITY_GROUP AS SINGLE_CITY
	,MAX(S.METROPOLITAN_NAME) AS SINGLE_CITY_MLU
FROM (
SELECT	
	METROPOLITAN_NAME	
	,LEFT(METROPOLITAN_NAME, CHARINDEX('-', METROPOLITAN_NAME) - 2)  AS CITY_GROUP
FROM [dbo].[MLU_METROPOLITAN]
WHERE
    METROPOLITAN_NAME <> 'UNKNOWN'
	AND CHARINDEX('/', METROPOLITAN_NAME) = 0
) S
GROUP BY
	S.CITY_GROUP
HAVING
	COUNT(*) = 1
"""

sql_metro_mlu = f"""
SELECT	
	METROPOLITAN_KEY
	,METROPOLITAN_NAME AS METROPOLITAN_NAME_MLU
FROM [dbo].[MLU_METROPOLITAN]
WHERE
    METROPOLITAN_NAME <> 'UNKNOWN'
"""


server_class = cfx.ConnectSQLServer(server=server
                                    , driver=driver
                                    , uname=uname
                                    , pword=pword)

with server_class.connection(authentication='sql server', db=source_db) as conn:
    df_metro_source = pd.read_sql(sql=sql_source_metro, con=conn)
    df_metro_one_city_one_state = pd.read_sql(sql=sql_metro_one_city_one_state, con=conn)
    df_metro_mlu = pd.read_sql(sql=sql_metro_mlu, con=conn)

df_metro_source['METRO_CLEAN'] = df_metro_source.apply(lambda x: fx_metro_clean(x['METROPOLITAN_SOURCE']), axis=1)
df_metro_source['SINGLE_CITY'] = df_metro_source['METRO_CLEAN']

df_metro = df_metro_source.merge(right=df_metro_one_city_one_state
                                 , how='left'
                                 , on=['SINGLE_CITY'])

df_metro['METROPOLITAN_NEW'] = df_metro.apply(lambda x: fx_metro_new(
    x['SINGLE_CITY_MLU']
    , x['METRO_CLEAN']
), axis=1)

df_metro_mlu['METROPOLITAN_NEW'] = df_metro_mlu['METROPOLITAN_NAME_MLU']

df_metro = df_metro.merge(right=df_metro_mlu[['METROPOLITAN_KEY', 'METROPOLITAN_NEW']]
                                 , how='left'
                                 , on=['METROPOLITAN_NEW']).copy()

df_metro['STATE_CODE'] = df_metro.apply(lambda x: fx_state_code(
    x['METROPOLITAN_NEW']
), axis=1)

column_out = [
'METROPOLITAN_SOURCE'
,'METROPOLITAN_NEW'
, 'STATE_CODE'
, 'METROPOLITAN_KEY'
]

df_metro = df_metro[column_out]



out_file = r'c:\temp\1.xlsx'
df_metro.to_excel(out_file, index=False)
os.startfile(out_file)
