import pandas as pd
import os
import csv
import glob
import pyodbc
import sqlalchemy
import datetime

source_directory = r'C:\Users\jasonwalker\OneDrive - SitusAMC\Documents\project\baseline\followingmonthcsvfiles'
out_directory = r'C:\Users\jasonwalker\OneDrive - SitusAMC\Documents\project\baseline\followingmonthcsvfiles\out'
stg_server = r'AMC-DLKDWP01.amcfirst.com'
stg_db = r'DW_LOAN_ATTRIBUTION'
dsn = r'DW_PROD_DW_LOAN_VALUATION'
report_date = '2021-12-01'
target_table = r'HMBS_PORTFOLIO_LOAN_LEVEL'

# file cleanup
for file in os.scandir(out_directory):
    os.remove(file.path)

file_size = {}
total_file_size = 0
for filename in glob.glob(source_directory + '/*.csv'):
    file = os.path.join(source_directory, filename)
    total_file_size += os.path.getsize(file)
    file_size[filename] = os.path.getsize(file)

file_size_split = total_file_size / 50

file_size_category = []
file_size_counter = 0
category = 1
for x in file_size:
    file = x
    file_size_iter = file_size[x]
    if file_size_counter >= file_size_split:
        category += 1
        file_size_counter = 0
    else:
        file_size_counter += file_size_iter
    file_size_category.append([file, file_size_iter, category])

category = set(x[2] for x in file_size_category)

# with pyodbc.connect(
#         "Driver={SQL Server};"
#         f"Server={stg_server[1:-1]};"
#         f"Database={stg_db[1:-1]};"
#         "UID=python;"
#         "PWD=FZ9H2Pcg=z%-cf?$;"
#         , autocommit=True) as conn01:
#
#     cursor = conn01.cursor()


engine = sqlalchemy.create_engine(
        f"mssql+pyodbc://@{dsn}",
        fast_executemany=True)

conn = engine.connect()

for x in category:
    category_iter = x
    out_file_iter = os.path.join(out_directory, 'out_file_' + str(category_iter) + '.csv')
    files = [x[0] for x in file_size_category if x[2] == category_iter]
    with open(out_file_iter, 'a') as a:
        for file in files:
            with open(file, 'r') as r:
                content = r.readlines()
                for line in content:
                    a.write(line)

    columns_in = ['SEQUENCE_NUMBER_SUFFIX'
        , 'DISCLOSURE_SEQUENCE_NUMBER'
        , 'POOL_ID'
        , 'CURRENT_PRINCIPAL_LIMIT'
        , 'PAYMENT_REASON_CODE'
        , 'CURRENT_HECM_LOAN_BALANCE'
        , 'HECM_ORIGINAL_FUNDING_DATE'
        , 'ORIGINAL_HECM_LOAN_BALANCE'
        , 'MIP_BASIS_POINTS'
        , 'CURRENT_MONTH_LIQUIDATION_FLAG'
        , 'MAXIMUM_CLAIM_AMOUNT'
        , 'HECM_LOAN_CURRENT_INTEREST_RATE'
        , 'AS_OF_DATE'
        , 'REMAINING_AVAILABLE_LINE_OF_CREDIT_AMOUNT'
        , 'ORIGINAL_AVAILABLE_LINE_OF_CREDIT_AMOUNT'
        , 'PARTICIPATION_UPB'
        , 'PARTICIPATION_INTEREST_RATE'
        , 'MONTHLY_SERVICING_FEE_AMOUNT'
        , 'HECM_ORIGINAL_FUNDING_DATE_2'
        , 'PAYMENT_REASON_CODE_2'
        , 'PROPERTY_CHARGE_SET_ASIDE_AMOUNT'
        , 'CREDIT_LINE_SET_ASIDE_AMOUNT'
        , 'INITIAL_MONTHLY_SCHEDULED_PAYMENT'
        , 'INITIAL_REMAINING_AVAILABLE_LINE_OF_CREDIT'
                  ]

    columns_new = ['SEQUENCE_NUMBER_SUFFIX'
        , 'DISCLOSURE_SEQUENCE_NUMBER'
        , 'POOL_ID'
        , 'CURRENT_PRINCIPAL_LIMIT'
        , 'PAYMENT_REASON_CODE'
        , 'CURRENT_HECM_LOAN_BALANCE'
        , 'HECM_ORIGINAL_FUNDING_DATE'
        , 'ORIGINAL_HECM_LOAN_BALANCE'
        , 'MIP_BASIS_POINTS'
        , 'CURRENT_MONTH_LIQUIDATION_FLAG'
        , 'MAXIMUM_CLAIM_AMOUNT'
        , 'HECM_LOAN_CURRENT_INTEREST_RATE'
        , 'AS_OF_DATE'
        , 'REMAINING_AVAILABLE_LINE_OF_CREDIT_AMOUNT'
        , 'ORIGINAL_AVAILABLE_LINE_OF_CREDIT_AMOUNT'
        , 'PARTICIPATION_UPB'
        , 'PARTICIPATION_INTEREST_RATE'
        , 'MONTHLY_SERVICING_FEE_AMOUNT'
        , 'PROPERTY_CHARGE_SET_ASIDE_AMOUNT'
        , 'CREDIT_LINE_SET_ASIDE_AMOUNT'
        , 'INITIAL_MONTHLY_SCHEDULED_PAYMENT'
        , 'INITIAL_REMAINING_AVAILABLE_LINE_OF_CREDIT'
        , 'REPORT_DATE'
                   ]

    df = pd.read_csv(out_file_iter, names=columns_in, keep_default_na=False)
    df['REPORT_DATE'] = [str(report_date) for x in range(len(df.index))]
    df['HECM_ORIGINAL_FUNDING_DATE'] = pd.to_datetime(df['HECM_ORIGINAL_FUNDING_DATE'], errors='coerce')
    df['AS_OF_DATE'] = pd.to_datetime(df['AS_OF_DATE'], errors='coerce')
    df_2 = df[columns_new]
    df_2.to_sql(target_table, conn, if_exists='append', index=False)

    # insert_table = r'INSERT INTO DW_LOAN_ATTRIBUTION.stage.VECTORS_FOLLOWING_MONTH_HISTORY '
    # insert_column = ', '.join(df_2.columns)
    # insert_value = ', '.join(['?' for x in range(len(df_2.columns))])
    # insert_sql = f'{insert_table} ({insert_column}) VALUES({insert_value});'

    print('success')
