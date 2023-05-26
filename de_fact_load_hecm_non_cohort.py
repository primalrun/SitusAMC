import pyodbc
import pandas as pd
import datetime
import sys

server = r'COM-DLKDWD01.SITUSAMC.COM'
db = r'DW_LOAN_VALUATION'
data_source_name = 'GINNIE MAE'
as_of_date = datetime.datetime(2017, 5, 1)


as_of_date = datetime.datetime.strftime(as_of_date, '%Y-%m-%d')

conn01 = pyodbc.connect(
    "Driver={ODBC Driver 17 for SQL Server};"
    f"Server={server};"
    f"Database={db};"
    "UID=python;"
    "PWD=BPcpfhSx0t2669St2jDX;"
    , autocommit=True)

def get_client_id(row, issuer_client_dict):
    if row['ISSUER'] in issuer_client_dict.keys():
        return issuer_client_dict[row['ISSUER']]
    else:
        return 100000

def get_hecm_original_funding_date_key(row, date_dict):
    date_iter = row['HECM_ORIGINAL_FUNDING_DATE']
    date_str = datetime.datetime.strftime(date_iter, '%Y-%m-%d')
    return date_dict[date_str]

def get_print_date_key(row, date_dict):
    date_iter = row['PRINT_DATE']
    date_str = datetime.datetime.strftime(date_iter, '%Y-%m-%d')
    return date_dict[date_str]

def get_as_of_date_key(row, date_dict):
    date_iter = row['AS_OF_DATE']
    date_str = datetime.datetime.strftime(date_iter, '%Y-%m-%d')
    return date_dict[date_str]

sql_data = f"""
SELECT	
	DL.IssuerID AS ISSUER
	,DL.HECMOriginalFundingDate AS HECM_ORIGINAL_FUNDING_DATE
	,DATEADD(MONTH, 2,V.AsOfDate) AS PRINT_DATE
	,V.AsOfDate AS AS_OF_DATE
	,DL.InitialPLU AS INITIAL_PLU
	,DL.MaximumClaimAmount AS MAXIMUM_CLAIM_AMOUNT
	,DL.MonthlyServicingFeeAmount AS MONTHLY_SERVICING_FEE_AMOUNT
	,DL.OriginalHECMLoanBalance AS ORIGINAL_HECM_LOAN_BALANCE
	,DL.OriginalDrawAmount AS ORIGINAL_DRAW_AMOUNT
	,DL.OriginalPrincipalLimit AS ORIGINAL_PRINCIPAL_LIMIT
	,DL.ExpectedAverageMortgageInterestRate AS EXPECTED_AVERAGE_MORTGAGE_INTEREST_RATE
	,DL.YoungestBorrowerAgeatPoolIssuance AS YOUNGEST_BORROWER_AGE_AT_POOL_ISSUANCE
	,DL.Borrower1AgeatPoolIssuance AS BORROWER_1_AGE_AT_POOL_ISSUANCE
	,DL.Borrower2AgeatPoolIssuance AS BORROWER_2_AGE_AT_POOL_ISSUANCE
	,DL.HECMLoanPaymentOptionCode AS HECM_LOAN_PAYMENT_OPTION
	,DL.PropertyValuationAmount AS PROPERTY_VALUATION_AMOUNT
	,DL.RateResetFrequency AS RATE_RESET_FREQUENCY
	,CASE
		WHEN (DATEDIFF(MONTH, DL.HECMOriginalFundingDate, V.AsOfDate) + 1) > 1 THEN (DATEDIFF(MONTH, DL.HECMOriginalFundingDate, V.AsOfDate) + 1)
		WHEN DL.HECMOriginalFundingDate IS NULL  THEN NULL
		ELSE 1 
		END AS PREPAY_SEASONING		
	,CASE
		WHEN DL.MIPBasisPoints = 0.005 THEN 2017
		ELSE 2014
		END AS PLF
	,CASE
		WHEN (DATEDIFF(MONTH, DL.HECMOriginalFundingDate, V.AsOfDate))  > 1 THEN (DATEDIFF(MONTH, DL.HECMOriginalFundingDate, V.AsOfDate))
		WHEN DL.HECMOriginalFundingDate IS NULL  THEN NULL
		ELSE 1 
		END AS DRAW_SEASONING		
	,SUM(V.ScheduledBalance) AS SCHEDULED_BALANCE
	,SUM(V.PartialPrepay) AS PARTIAL_PREPAY
	,SUM(V.FullPrepay) AS FULL_PREPAY
	,SUM(1) AS NUMBER_OF_RECORDS
	,SUM(V.ParticipationUPBFollowingMonth) AS PARTICIPATION_UPB_FOLLOWING_MONTH
	,SUM(V.CurrentHECMUPB) AS CURRENT_HECM_UPB
FROM stage.BASELINE_DIRTY_LOANS_HISTORY_1 DL WITH(NOLOCK)
INNER JOIN stage.BASELINE_VECTORS_HISTORY_1 V WITH(NOLOCK) ON DL.DisclosureSequenceNumber = V.DisclosureSequenceNumber
WHERE
	V.AsOfDate = '{as_of_date}'
GROUP BY 
	DATEADD(MONTH, 2,V.AsOfDate)
	,DL.IssuerID
	,DL.HECMOriginalFundingDate
	,V.AsOfDate
	,DL.InitialPLU
	,DL.MaximumClaimAmount
	,DL.MonthlyServicingFeeAmount
	,DL.OriginalHECMLoanBalance
	,DL.OriginalDrawAmount
	,DL.OriginalPrincipalLimit
	,DL.ExpectedAverageMortgageInterestRate
	,DL.YoungestBorrowerAgeatPoolIssuance
	,DL.Borrower1AgeatPoolIssuance
	,DL.Borrower2AgeatPoolIssuance
	,DL.HECMLoanPaymentOptionCode
	,DL.PropertyValuationAmount
	,DL.RateResetFrequency
	,CASE
		WHEN (DATEDIFF(MONTH, DL.HECMOriginalFundingDate, V.AsOfDate) + 1) > 1 THEN (DATEDIFF(MONTH, DL.HECMOriginalFundingDate, V.AsOfDate) + 1)
		WHEN DL.HECMOriginalFundingDate IS NULL  THEN NULL
		ELSE 1 
		END
	,CASE
		WHEN DL.MIPBasisPoints = 0.005 THEN 2017
		ELSE 2014
		END
	,CASE
		WHEN (DATEDIFF(MONTH, DL.HECMOriginalFundingDate, V.AsOfDate))  > 1 THEN (DATEDIFF(MONTH, DL.HECMOriginalFundingDate, V.AsOfDate))
		WHEN DL.HECMOriginalFundingDate IS NULL  THEN NULL
		ELSE 1 
		END
"""

data_df = pd.read_sql(sql=sql_data, con=conn01)

sql_db_data = f"""
SELECT
	SOURCE_SID
	,DATABASE_PRINCIPAL_ID() AS CREATED_USER
FROM DW_DIMENSIONS.dbo.MLU_DATA_SOURCE WITH(NOLOCK)
WHERE SOURCE_NAME = '{data_source_name}'
"""

db_data_df = pd.read_sql(sql=sql_db_data, con=conn01)
data_source = db_data_df.iloc[0, 0]
created_user = db_data_df.iloc[0, 1]

sql_issuer_mlu = """
SELECT
    ISSUER_ID
    ,ISSUER
FROM stage.DIM_GINNIE_MAE_ISSUER
"""

issuer_dict = pd.read_sql(sql=sql_issuer_mlu, con=conn01).set_index('ISSUER').T.to_dict('records')[0]


sql_client_id = f"""
SELECT
    CLIENT_ALIAS AS ISSUER
    ,CLIENT_KEY
FROM DW_DIMENSIONS.dbo.REF_CLIENT WITH(NOLOCK)
WHERE
    DATA_SOURCE = {data_source}
"""

issuer_client_dict = pd.read_sql(sql=sql_client_id, con=conn01).set_index('ISSUER').T.to_dict('records')[0]

sql_date_key = """
SELECT
	CONVERT(DATE, DATE) AS DATE
	,DATE_KEY
FROM dbo.vw_DIM_DATE WITH(NOLOCK)
"""

date_dict = pd.read_sql(sql=sql_date_key, con=conn01).set_index('DATE').T.to_dict('records')[0]
date_dict = {datetime.datetime.strftime(k, '%Y-%m-%d'): v for (k, v) in date_dict.items()}

# modify dataframe
data_df['DATA_SOURCE'] = [data_source for x in range(len(data_df.index))]
data_df['CREATED_USER'] = [created_user for x in range(len(data_df.index))]
data_df['ISSUER_ID'] = data_df.apply(lambda x: issuer_dict[x['ISSUER']], axis=1)
data_df['CLIENT_ID'] = data_df.apply(lambda x: get_client_id(x, issuer_client_dict), axis=1)

data_df['HECM_ORIGINAL_FUNDING_DATE_KEY'] = data_df.apply(lambda x: get_hecm_original_funding_date_key(x, date_dict), axis=1)
data_df['PRINT_DATE_KEY'] = data_df.apply(lambda x: get_print_date_key(x, date_dict), axis=1)
data_df['AS_OF_DATE_KEY'] = data_df.apply(lambda x: get_as_of_date_key(x, date_dict), axis=1)

created_date = datetime.datetime.now()
created_date = datetime.datetime.strftime(created_date, '%Y-%m-%d %H:%M:%S')
data_df['CREATED_DATE'] = [created_date for x in range(len(data_df.index))]

column_out = [
'CLIENT_ID'
,'DATA_SOURCE'
,'ISSUER_ID'
,'ISSUER'
,'HECM_ORIGINAL_FUNDING_DATE_KEY'
,'PRINT_DATE_KEY'
,'AS_OF_DATE_KEY'
,'INITIAL_PLU'
,'MAXIMUM_CLAIM_AMOUNT'
,'MONTHLY_SERVICING_FEE_AMOUNT'
,'ORIGINAL_HECM_LOAN_BALANCE'
,'ORIGINAL_DRAW_AMOUNT'
,'ORIGINAL_PRINCIPAL_LIMIT'
,'EXPECTED_AVERAGE_MORTGAGE_INTEREST_RATE'
,'YOUNGEST_BORROWER_AGE_AT_POOL_ISSUANCE'
,'BORROWER_1_AGE_AT_POOL_ISSUANCE'
,'BORROWER_2_AGE_AT_POOL_ISSUANCE'
,'HECM_LOAN_PAYMENT_OPTION'
,'PROPERTY_VALUATION_AMOUNT'
,'RATE_RESET_FREQUENCY'
,'PREPAY_SEASONING'
,'PLF'
,'DRAW_SEASONING'
,'SCHEDULED_BALANCE'
,'PARTIAL_PREPAY'
,'FULL_PREPAY'
,'NUMBER_OF_RECORDS'
,'PARTICIPATION_UPB_FOLLOWING_MONTH'
,'CURRENT_HECM_UPB'
,'CREATED_DATE'
,'CREATED_USER'
]

data_df = data_df[column_out]
print(data_df.head())
