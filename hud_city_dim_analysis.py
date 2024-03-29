import pandas as pd
import common_functions as cfx
import os
import sys

# variable
fact_table_db = r'DW_LOAN_VALUATION'




server, driver, uname, pword, dsn = cfx.environment_variables('prod')

sql_unmapped = f"""
DECLARE @DATA_SOURCE BIGINT = (SELECT SOURCE_SID FROM DW_DIMENSIONS.dbo.MLU_DATA_SOURCE WHERE SOURCE_NAME = 'DATA WAREHOUSE STANDARD DIMENSION');
DECLARE @ALIAS_TYPE VARCHAR(50) = 'CITY'
DECLARE @DIM_NULL_KEY BIGINT = (SELECT CITY_KEY FROM [DW_DIMENSIONS].[dbo].[MLU_CITY] WHERE CITY_NAME = 'UNKNOWN');
DECLARE @ENDORSEMENT_YEAR INT = (SELECT MAX(ENDORSEMENT_YEAR) FROM [DW_LOAN_VALUATION].[dbo].[vw_FACT_HECM_TPO_ENDORSEMENT_UNIVERSE] WITH(NOLOCK));
DECLARE @ENDORSEMENT_MONTH INT = (
	SELECT 
		MAX(ENDORSEMENT_MONTH) 
	FROM [DW_LOAN_VALUATION].[dbo].[vw_FACT_HECM_TPO_ENDORSEMENT_UNIVERSE] WITH(NOLOCK) 
	WHERE 
		ENDORSEMENT_YEAR = @ENDORSEMENT_YEAR
	);

SELECT	
	A.SOURCE_CITY
	,A.PARENT_KEY AS STATE_KEY
	,CASE
		WHEN CHARINDEX(' ', A.SOURCE_CITY) > 0
			THEN LEFT(A.SOURCE_CITY, CHARINDEX(' ', A.SOURCE_CITY) - 1)
			ELSE A.SOURCE_CITY
			END AS CITY_PREFIX_1	
FROM (
SELECT DISTINCT
	PROPERTY_CITY AS SOURCE_CITY
	,STATE_KEY AS PARENT_KEY
FROM [DW_LOAN_VALUATION].[stage].[HECM_ENDORSEMENT] WITH(NOLOCK)
WHERE
	NULLIF(PROPERTY_CITY, '') IS NOT NULL
	AND ENDORSEMENT_YEAR = @ENDORSEMENT_YEAR
	AND ENDORSEMENT_MONTH = @ENDORSEMENT_MONTH
) A
LEFT JOIN DW_DIMENSIONS.dbo.REF_ALIAS RA WITH(NOLOCK)
	ON RA.ALIAS_TYPE = @ALIAS_TYPE
	AND RA.DATA_SOURCE = @DATA_SOURCE
	AND A.PARENT_KEY = RA.PARENT_KEY
	AND A.SOURCE_CITY = RA.ALIAS
WHERE
	COALESCE(RA.DIM_KEY, @DIM_NULL_KEY) = @DIM_NULL_KEY
"""

sql_dim = f"""
DECLARE @COUNTRY_KEY_USA BIGINT = (SELECT COUNTRY_KEY FROM [DW_DIMENSIONS].[dbo].[MLU_COUNTRY] WITH(NOLOCK) WHERE COUNTRY_NAME = 'UNITED STATES')
DECLARE @DATA_SOURCE BIGINT = (SELECT SOURCE_SID FROM DW_DIMENSIONS.dbo.MLU_DATA_SOURCE WHERE SOURCE_NAME = 'DATA WAREHOUSE STANDARD DIMENSION');
DECLARE @ALIAS_TYPE VARCHAR(50) = 'CITY'

SELECT
	A.STATE_KEY
	,A.CITY_KEY
	,A.CITY_NAME
	,A.ALIAS
	,CASE
		WHEN CHARINDEX(' ', A.CITY_NAME) > 0
			THEN LEFT(A.CITY_NAME, CHARINDEX(' ', A.CITY_NAME) - 1)
			ELSE A.CITY_NAME
			END AS MLU_PREFIX_1
	,CASE
		WHEN CHARINDEX(' ', A.ALIAS) > 0
			THEN LEFT(A.ALIAS, CHARINDEX(' ', A.ALIAS) - 1)
			ELSE A.ALIAS
			END AS ALIAS_PREFIX_1	
FROM (
SELECT
	C.STATE_KEY
	,C.CITY_KEY	
	,UPPER(TRIM(C.CITY_NAME)) AS CITY_NAME
	,UPPER(TRIM(RA.ALIAS)) AS ALIAS
FROM [DW_DIMENSIONS].[dbo].[MLU_CITY] C WITH(NOLOCK)
LEFT JOIN [DW_DIMENSIONS].[dbo].[REF_ALIAS] RA WITH(NOLOCK)
	ON RA.ALIAS_TYPE = @ALIAS_TYPE
	AND RA.DATA_SOURCE = @DATA_SOURCE 
	AND C.STATE_KEY = RA.PARENT_KEY
	AND C.CITY_KEY = RA.DIM_KEY
WHERE
	C.COUNTRY_KEY = @COUNTRY_KEY_USA
	AND C.IS_INACTIVE IS NULL
) A
ORDER BY
	A.STATE_KEY
	,A.CITY_NAME
	,A.ALIAS
"""



server_class = cfx.ConnectSQLServer(server=server
                                    , driver=driver
                                    , uname=uname
                                    , pword=pword)


with server_class.connection(authentication='sql server', db=fact_table_db) as conn:
    df_unmapped = pd.read_sql(sql=sql_unmapped, con=conn)
    df_dim = pd.read_sql(sql=sql_dim, con=conn)

df_dim['CITY_PREFIX_1'] = df_dim['MLU_PREFIX_1']

df_search_result = df_unmapped.merge(
    right=df_dim[['STATE_KEY', 'CITY_KEY', 'CITY_NAME', 'ALIAS', 'CITY_PREFIX_1', 'MLU_PREFIX_1', 'ALIAS_PREFIX_1']]
        , how='left'
        , on=['STATE_KEY', 'CITY_PREFIX_1']
).copy()

df_search_result.sort_values(by=['STATE_KEY', 'SOURCE_CITY', 'CITY_NAME'], ascending=True, inplace=True)
out_file = r'c:\temp\hud_city_dim_analysis.csv'
df_search_result.to_csv(path_or_buf=out_file, index=False)
os.startfile(out_file)

