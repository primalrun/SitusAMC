import pandas as pd
import common_functions as cfx
import pyodbc
import os
import sys

# variable
fact_table_db = r'DW_LOAN_VALUATION'
source_dim_map_file = r'C:\Users\jasonwalker\OneDrive - SitusAMC\Documents\project\baseline\dim_mapping\DIM_MAPPING_FILE\CITY.xlsx'
data_source_name_alias = 'DATA WAREHOUSE STANDARD DIMENSION'
country_name_usa = 'UNITED STATES'
alias_type = 'CITY'

# source data
converter = {'SOURCE_CITY': str
             , 'STATE_KEY': int
             , 'MLU_EXISTS': int
             , 'DIM_KEY': int}
source_data = pd.read_excel(io=source_dim_map_file, header=0, converters=converter).values.tolist()

# end process if source data is missing
if len(source_data) == 0:
    print('No data in mapping file, process cancelled')
    sys.exit()

insert_values = ''

for x in source_data:
    insert_values += cfx.convert_nan_string_to_null(str(tuple(x))) + '\n,'

insert_values = insert_values[:-2]

server, driver, uname, pword, dsn = cfx.environment_variables('prod')



sql_mapping = f"""
DROP TABLE IF EXISTS [stage].[DIM_MAPPING_TEMP];

CREATE TABLE [stage].[DIM_MAPPING_TEMP] (
SOURCE_CITY VARCHAR(100)
,STATE_KEY BIGINT
,MLU_EXISTS BIT 
,DIM_KEY BIGINT
,DATA_SOURCE_ALIAS BIGINT
);

INSERT INTO [stage].[DIM_MAPPING_TEMP] ( 
SOURCE_CITY
,STATE_KEY
,MLU_EXISTS 
,DIM_KEY
)
VALUES
{insert_values}
;

"""


sql_mlu_insert = f"""
DECLARE @COUNTRY_KEY_USA BIGINT = (
SELECT
	COUNTRY_KEY
FROM DW_DIMENSIONS.dbo.MLU_COUNTRY WITH(NOLOCK)
WHERE
	COUNTRY_NAME = '{country_name_usa}'
);


INSERT INTO DW_DIMENSIONS.dbo.MLU_CITY WITH(TABLOCK) (
CITY_NAME
,MSA_DIV_KEY
,COMBINED_STATISTICAL_AREAS_KEY
,COUNTY_KEY
,STATE_KEY
,COUNTRY_KEY
,CITY_HASH
,CREATED_DATE
,CREATED_USER
)
SELECT
	S.SOURCE_CITY AS CITY_NAME
	,100000 AS MSA_DIV_KEY
	,100000 AS COMBINED_STATISTICAL_AREAS_KEY
	,100000 AS COUNTY_KEY
	,S.STATE_KEY
	,@COUNTRY_KEY_USA AS COUNTRY_KEY
	,HASHBYTES('SHA2_256',UPPER(TRIM(S.SOURCE_CITY))+STR(S.STATE_KEY)) AS CITY_HASH
	,GETDATE() AS CREATED_DATE
	,DATABASE_PRINCIPAL_ID() AS CREATED_USER
FROM [stage].[DIM_MAPPING_TEMP] S WITH(NOLOCK)
INNER JOIN (
SELECT
	STATE_KEY
	,SOURCE_CITY AS CITY_NAME
FROM [stage].[DIM_MAPPING_TEMP] WITH(NOLOCK)
WHERE
	MLU_EXISTS = 0

EXCEPT

SELECT
	STATE_KEY
	,CITY_NAME
FROM DW_DIMENSIONS.dbo.MLU_CITY WITH(NOLOCK)
WHERE
	COUNTRY_KEY = @COUNTRY_KEY_USA
) E
	ON S.STATE_KEY = E.STATE_KEY
	AND S.SOURCE_CITY = E.CITY_NAME
WHERE
	MLU_EXISTS = 0
;
"""

sql_update_dim_key = f"""
DECLARE @COUNTRY_KEY_USA BIGINT = (
SELECT
	COUNTRY_KEY
FROM DW_DIMENSIONS.dbo.MLU_COUNTRY WITH(NOLOCK)
WHERE
	COUNTRY_NAME = '{country_name_usa}'
);


UPDATE DT
SET
    DIM_KEY = MC.CITY_KEY
FROM [stage].[DIM_MAPPING_TEMP] DT
INNER JOIN DW_DIMENSIONS.dbo.MLU_CITY MC WITH(NOLOCK)
    ON MC.COUNTRY_KEY = @COUNTRY_KEY_USA
    AND DT.STATE_KEY = MC.STATE_KEY
    AND DT.SOURCE_CITY  = MC.CITY_NAME
WHERE
    DT.MLU_EXISTS = 0
;
"""


server_class = cfx.ConnectSQLServer(server=server
                                    , driver=driver
                                    , uname=uname
                                    , pword=pword)


with server_class.connection(authentication='sql server', db=fact_table_db) as conn:
    cursor = conn.cursor()
    cursor.execute(sql_mapping)
    cursor.execute(sql_mlu_insert)
    cursor.execute(sql_update_dim_key)

print('complete')
