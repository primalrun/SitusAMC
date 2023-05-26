import pandas as pd
import common_functions as cfx
import os
import sys

# variable
fact_table_db = r'DW_LOAN_VALUATION'
data_source_name = r'U.S. DEPARTMENT OF HOUSING AND URBAN DEVELOPMENT'
account_type = 'ISSUER'


server, driver, uname, pword, dsn = cfx.environment_variables('prod')

sql_unmapped = f"""
DECLARE @DATA_SOURCE BIGINT = (SELECT SOURCE_SID FROM [DW_DIMENSIONS].[dbo].[MLU_DATA_SOURCE] WHERE SOURCE_NAME = '{data_source_name}');
DECLARE @ACCOUNT_TYPE VARCHAR(50) = '{account_type}'
DECLARE @DIM_NULL_KEY BIGINT = (SELECT CLIENT_KEY FROM [DW_DIMENSIONS].[dbo].[MLU_CLIENT] WHERE CLIENT_NAME = 'UNKNOWN');
DECLARE @ENDORSEMENT_YEAR INT = (SELECT MAX(ENDORSEMENT_YEAR) FROM [DW_LOAN_VALUATION].[dbo].[vw_FACT_HECM_TPO_ENDORSEMENT_UNIVERSE] WITH(NOLOCK));
DECLARE @ENDORSEMENT_MONTH INT = (
	SELECT 
		MAX(ENDORSEMENT_MONTH) 
	FROM [DW_LOAN_VALUATION].[dbo].[vw_FACT_HECM_TPO_ENDORSEMENT_UNIVERSE] WITH(NOLOCK) 
	WHERE 
		ENDORSEMENT_YEAR = @ENDORSEMENT_YEAR
	);


SELECT	
	A.SOURCE_ISSUER		
	,CASE
		WHEN CHARINDEX(' ', A.SOURCE_ISSUER) > 0
			THEN LEFT(A.SOURCE_ISSUER, CHARINDEX(' ', A.SOURCE_ISSUER) - 1)
			ELSE A.SOURCE_ISSUER
			END AS SOURCE_ISSUER_PREFIX_1		
FROM (
SELECT DISTINCT
	ISSUER AS SOURCE_ISSUER
FROM [DW_LOAN_VALUATION].[stage].[HECM_ENDORSEMENT] WITH(NOLOCK)
WHERE
	NULLIF(ISSUER, '') IS NOT NULL
	AND ENDORSEMENT_YEAR = @ENDORSEMENT_YEAR
	AND ENDORSEMENT_MONTH = @ENDORSEMENT_MONTH
) A
LEFT JOIN DW_DIMENSIONS.dbo.REF_CLIENT RC WITH(NOLOCK)
	ON RC.ACCOUNT_TYPE = @ACCOUNT_TYPE
	AND RC.DATA_SOURCE = @DATA_SOURCE
	AND A.SOURCE_ISSUER = RC.CLIENT_ALIAS
WHERE
	COALESCE(RC.CLIENT_KEY, @DIM_NULL_KEY) = @DIM_NULL_KEY
"""

sql_dim = f"""
DECLARE @DATA_SOURCE BIGINT = (SELECT SOURCE_SID FROM DW_DIMENSIONS.dbo.MLU_DATA_SOURCE WHERE SOURCE_NAME = '{data_source_name}');
DECLARE @ACCOUNT_TYPE VARCHAR(50) = '{account_type}'

SELECT
	MC.CLIENT_KEY
	,MC.CLIENT_NAME
	,RC.CLIENT_ALIAS
	,CASE
		WHEN CHARINDEX(' ', MC.CLIENT_NAME) > 0
			THEN LEFT(MC.CLIENT_NAME, CHARINDEX(' ', MC.CLIENT_NAME) - 1)
			ELSE MC.CLIENT_NAME
			END AS MLU_PREFIX_1
	,CASE
		WHEN CHARINDEX(' ', RC.CLIENT_ALIAS) > 0
			THEN LEFT(RC.CLIENT_ALIAS, CHARINDEX(' ', RC.CLIENT_ALIAS) - 1)
			ELSE RC.CLIENT_ALIAS
			END AS ALIAS_PREFIX_1
FROM [DW_DIMENSIONS].[dbo].[MLU_CLIENT] MC WITH(NOLOCK)
LEFT JOIN [DW_DIMENSIONS].[dbo].[REF_CLIENT] RC WITH(NOLOCK)
	ON RC.ACCOUNT_TYPE = @ACCOUNT_TYPE
	AND RC.DATA_SOURCE = @DATA_SOURCE 	
	AND MC.CLIENT_KEY = RC.CLIENT_KEY
WHERE	
	MC.IS_INACTIVE IS NULL
ORDER BY	
	MC.CLIENT_NAME
	,RC.CLIENT_ALIAS
"""



server_class = cfx.ConnectSQLServer(server=server
                                    , driver=driver
                                    , uname=uname
                                    , pword=pword)


with server_class.connection(authentication='sql server', db=fact_table_db) as conn:
    df_unmapped = pd.read_sql(sql=sql_unmapped, con=conn)
    df_dim = pd.read_sql(sql=sql_dim, con=conn)

df_dim['SOURCE_ISSUER_PREFIX_1'] = df_dim['MLU_PREFIX_1']


df_search_result = df_unmapped.merge(
    right=df_dim
        , how='left'
        , on=['SOURCE_ISSUER_PREFIX_1']
).copy()


df_search_result.sort_values(by=['SOURCE_ISSUER', 'SOURCE_ISSUER_PREFIX_1', 'CLIENT_NAME', 'CLIENT_ALIAS'], ascending=True)
out_file = r'c:\temp\hud_issuer_dim_analysis.csv'
df_search_result.to_csv(path_or_buf=out_file, index=False)
os.startfile(out_file)

