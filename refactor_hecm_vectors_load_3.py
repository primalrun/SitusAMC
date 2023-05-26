import pandas as pd
import pyodbc
import time
import sys


server = r'COM-DLKDWD01.SITUSAMC.COM'
db = r'DW_LOAN_VALUATION'

conn01 = pyodbc.connect(
    "Driver={ODBC Driver 17 for SQL Server};"
    f"Server={server};"
    f"Database={db};"
    "UID=python;"
    "PWD=BPcpfhSx0t2669St2jDX;"
    , autocommit=True)


def calc_assignment(participation_upb_fm,
                    participation_upb_cm,
                    current_hecm_upb,
                    mip_basis_points,
                    hecm_loan_interest_rate,
                    max_claim_amount,
                    current_interest_rate):
    if (participation_upb_fm == 0
            and participation_upb_cm > .01
            and (current_hecm_upb * (
                    1 + ((mip_basis_points + hecm_loan_interest_rate) / 12))) / max_claim_amount >= .975):
        return participation_upb_cm * (1 + (current_interest_rate / 12))
    else:
        return 0


def calc_scheduled_balance(participation_upb_cm,
                           scheduled_interest_rate,
                           current_hecm_upb,
                           assignment):
    return (participation_upb_cm * (1 + (scheduled_interest_rate / 12))) - assignment


def calc_full_prepay(participation_upb_fm,
                     scheduled_balance):
    if (participation_upb_fm == 0):
        return scheduled_balance
    else:
        return 0


def calc_partial_prepay(participation_upb_cm,
                        scheduled_balance,
                        participation_upb_fm,
                        full_prepay):
    if participation_upb_cm > 1:
        return max(0, (scheduled_balance - participation_upb_fm - full_prepay))
    else:
        return 0


def dataframe_to_generator(df_parm):
    # modify main data dataframe--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    df_parm['ASSIGNMENT'] = df_parm.apply(lambda x: calc_assignment(x['PARTICIPATION_UPB_FOLLOWING_MONTH']
                                                                    , x['PARTICIPATION_UPB_CURRENT_MONTH']
                                                                    , x['CURRENT_HECM_UPB']
                                                                    , x['MIP_BASIS_POINTS']
                                                                    , x['HECM_LOAN_CURRENT_INTEREST_RATE']
                                                                    , x['MAXIMUM_CLAIM_AMOUNT']
                                                                    , x['CURRENT_INTEREST_RATE'])
                                          , axis=1)
    df_parm['SCHEDULED_BALANCE'] = df_parm.apply(
        lambda x: calc_scheduled_balance(x['PARTICIPATION_UPB_CURRENT_MONTH']
                                         , x['SCHEDULED_INTEREST_RATE']
                                         , x['CURRENT_HECM_UPB']
                                         , x['ASSIGNMENT'])
        , axis=1)
    df_parm['FULL_PREPAY'] = df_parm.apply(lambda x: calc_full_prepay(x['PARTICIPATION_UPB_FOLLOWING_MONTH']
                                                                      , x['SCHEDULED_BALANCE'])
                                           , axis=1)
    df_parm['PARTIAL_PREPAY'] = df_parm.apply(lambda x: calc_partial_prepay(x['PARTICIPATION_UPB_CURRENT_MONTH']
                                                                            , x['SCHEDULED_BALANCE']
                                                                            , x['PARTICIPATION_UPB_FOLLOWING_MONTH']
                                                                            , x['FULL_PREPAY'])
                                              , axis=1)

    df_parm['AS_OF_DATE'] = pd.to_datetime(df_parm['AS_OF_DATE'])

    # prepare dataframe for output to sql server-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    column_out = [
        'POOL_ID'
        , 'DISCLOSURE_SEQUENCE_NUMBER'
        , 'AS_OF_DATE_KEY'
        , 'AS_OF_DATE'
        , 'SEQUENCE_NUMBER_SUFFIX'
        , 'MIP_BASIS_POINTS'
        , 'MONTHLY_SERVICING_FEE_AMOUNT'
        , 'MAXIMUM_CLAIM_AMOUNT'
        , 'HECM_LOAN_CURRENT_INTEREST_RATE'
        , 'CURRENT_HECM_UPB'
        , 'PARTICIPATION_UPB_FOLLOWING_MONTH'
        , 'CURRENT_HECM_NOTE_RATE'
        , 'CURRENT_LINE_OF_CREDIT'
        , 'FOLLOWING_MONTH_LINE_OF_CREDIT'
        , 'ASSIGNMENT'
        , 'SCHEDULED_BALANCE'
        , 'PARTIAL_PREPAY'
        , 'FULL_PREPAY'
    ]

    df_parm = df_parm[column_out]
    data_temp = tuple(map(tuple, df_parm.to_records(index=False)))
    for _ in data_temp:
        yield _


sql_data = f"""
SELECT TOP 100000
	P.POOL_ID
	,P.DISCLOSURE_SEQUENCE_NUMBER
	,D.DATE_KEY AS AS_OF_DATE_KEY
	,P.AS_OF_DATE
	,P.SEQUENCE_NUMBER_SUFFIX
	,P.MIP_BASIS_POINTS
	,COALESCE(P.MONTHLY_SERVICING_FEE_AMOUNT, 0) AS MONTHLY_SERVICING_FEE_AMOUNT
	,COALESCE(P.MAXIMUM_CLAIM_AMOUNT, 0) AS MAXIMUM_CLAIM_AMOUNT
	,COALESCE(P.HECM_LOAN_CURRENT_INTEREST_RATE, 0) AS HECM_LOAN_CURRENT_INTEREST_RATE
	,P.CURRENT_HECM_LOAN_BALANCE AS CURRENT_HECM_UPB
	,P.PARTICIPATION_UPB AS PARTICIPATION_UPB_CURRENT_MONTH
	,COALESCE(C.PARTICIPATION_UPB, 0) AS PARTICIPATION_UPB_FOLLOWING_MONTH
	,COALESCE(P.HECM_LOAN_CURRENT_INTEREST_RATE, 0) AS CURRENT_HECM_NOTE_RATE
	,P.REMAINING_AVAILABLE_LINE_OF_CREDIT_AMOUNT AS CURRENT_LINE_OF_CREDIT
	,C.REMAINING_AVAILABLE_LINE_OF_CREDIT_AMOUNT AS FOLLOWING_MONTH_LINE_OF_CREDIT
	,COALESCE(P.PARTICIPATION_INTEREST_RATE, 0) AS CURRENT_INTEREST_RATE
	,COALESCE(C.PARTICIPATION_INTEREST_RATE, 0) AS SCHEDULED_INTEREST_RATE	
FROM [stage].[HMBS_PORTFOLIO_LOAN_LEVEL] P WITH(NOLOCK)
LEFT JOIN [stage].[HMBS_PORTFOLIO_LOAN_LEVEL] C WITH(NOLOCK)
	ON P.DISCLOSURE_SEQUENCE_NUMBER = C.DISCLOSURE_SEQUENCE_NUMBER
	AND P.SEQUENCE_NUMBER_SUFFIX = C.SEQUENCE_NUMBER_SUFFIX
	AND C.AS_OF_DATE = '2021-12-01'
LEFT JOIN dbo.vw_DIM_DATE D WITH(NOLOCK)
	ON P.AS_OF_DATE = D.DATE
WHERE
	P.AS_OF_DATE = '2021-11-01'
"""



start = time.time()
query_generator = pd.read_sql(sql=sql_data, con=conn01, chunksize=10000)
print('byte size of query_generator = ', sys.getsizeof(query_generator))

data_cleansed = (dataframe_to_generator(pd.DataFrame(gen)) for gen in query_generator)
print('byte size of data_cleansed = ', sys.getsizeof(data_cleansed))

column_out = [
    'POOL_ID'
    , 'DISCLOSURE_SEQUENCE_NUMBER'
    , 'AS_OF_DATE_KEY'
    , 'AS_OF_DATE'
    , 'SEQUENCE_NUMBER_SUFFIX'
    , 'MIP_BASIS_POINTS'
    , 'MONTHLY_SERVICING_FEE_AMOUNT'
    , 'MAXIMUM_CLAIM_AMOUNT'
    , 'HECM_LOAN_CURRENT_INTEREST_RATE'
    , 'CURRENT_HECM_UPB'
    , 'PARTICIPATION_UPB_FOLLOWING_MONTH'
    , 'CURRENT_HECM_NOTE_RATE'
    , 'CURRENT_LINE_OF_CREDIT'
    , 'FOLLOWING_MONTH_LINE_OF_CREDIT'
    , 'ASSIGNMENT'
    , 'SCHEDULED_BALANCE'
    , 'PARTIAL_PREPAY'
    , 'FULL_PREPAY'
]



df_input_gen = tuple(data_row for query_gen in data_cleansed for data_row in query_gen)
print('byte size of df_input_gen = ', sys.getsizeof(df_input_gen))

data_df = pd.DataFrame(df_input_gen, columns=column_out)
print('byte size of data_df = ', sys.getsizeof(data_df))
end = time.time()
duration = end - start

print(data_df)
print(duration)


