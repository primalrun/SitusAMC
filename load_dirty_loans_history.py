import pandas as pd
import numpy as np
import datetime
import pyodbc

source_file = r'C:\Users\jasonwalker\OneDrive - SitusAMC\Documents\project\baseline\history_file\dirtyLoans12.08.20.csv'

server = r'COM-DLKDWD01.SITUSAMC.COM'
db = r'DW_LOAN_VALUATION'

conn01 = pyodbc.connect(
    "Driver={SQL Server};"
    f"Server={server};"
    f"Database={db};"
    , autocommit=True)

table_name = r'dbo.DIRTY_LOANS'

data_type = {'Disclosure Sequence Number': int,
             'Payment Reason Code': str,
             'HECM Original Funding Date': object,
             'Original HECM Loan Balance': float,
             'MIP Basis Points': float,
             'Maximum Claim Amount': float,
             'Original Available Line of Credit Amount': float,
             'Age': str,
             'Pool ID': str,
             'Issuer ID': str,
             'Property Type Code': str,
             'Original Principal Limit': float,
             'Monthly Servicing Fee Amount': float,
             'Mortgage Margin': float,
             'HECM Loan Original Interest Rate': float,
             'Rate Reset Frequency': str,
             'Servicing Fee Margin': float,
             'Borrower 1 Age at Pool Issuance': str,
             'Borrower 2 Age at Pool Issuance': str,
             'Loan Servicing Fee Code': str,
             'Property Valuation Amount': float,
             'Property Valuation Effective Date': str,
             'Loan Origination Company': str,
             'HECM Loan Purpose Code': str,
             'HECM Loan Payment Option Code': str,
             'HECM Saver Flag': str,
             'Original Draw Amount': float,
             'Mortgage Servicer': str,
             'Lifetime Floor Rate': float,
             'Initial Change Date': str,
             'Lifetime Interest Rate Change Cap': float,
             'Reset Months': str,
             'Eligible Non-borrowing Spouse': str,
             'Annual Interest Rate Change Cap': float,
             'Maximum Interest Rate': float,
             'Mandatory Property Charges Set Aside': str,
             'Youngest Borrower Age at Pool Issuance': str,
             'Initial PLU': float,
             'Expected Average Mortgage Interest Rate': float
             }

dirty_load_df = pd.read_csv(filepath_or_buffer=source_file, delimiter=',', dtype=data_type)
dirty_load_df['Age'] = dirty_load_df['Age'].astype(int)
dirty_load_df['Borrower 1 Age at Pool Issuance'] = dirty_load_df['Borrower 1 Age at Pool Issuance'].astype(int)
dirty_load_df['Borrower 2 Age at Pool Issuance'] = dirty_load_df['Borrower 1 Age at Pool Issuance'].astype(int)
dirty_load_df['Reset Months'] = dirty_load_df['Reset Months'].replace(r'^\s*$', np.NaN, regex=True)
dirty_load_df['Youngest Borrower Age at Pool Issuance'] = dirty_load_df[
    'Youngest Borrower Age at Pool Issuance'].astype(int)
dirty_load_df['HECM Original Funding Date'] = pd.to_datetime(dirty_load_df['HECM Original Funding Date'],
                                                             errors='coerce')
dirty_load_df['Property Valuation Effective Date'] = pd.to_datetime(dirty_load_df['Property Valuation Effective Date'],
                                                                    errors='coerce')
dirty_load_df['Initial Change Date'] = pd.to_datetime(dirty_load_df['Initial Change Date'], errors='coerce')

new_columns = (
    'DISCLOSURE_SEQUENCE_NUMBER'
    , 'PAYMENT_REASON_CODE'
    , 'HECM_ORIGINAL_FUNDING_DATE'
    , 'ORIGINAL_HECM_LOAN_BALANCE'
    , 'MIP_BASIS_POINTS'
    , 'MAXIMUM_CLAIM_AMOUNT'
    , 'ORIGINAL_AVAILABLE_LINE_OF_CREDIT_AMOUNT'
    , 'AGE'
    , 'POOL_ID'
    , 'ISSUER_ID'
    , 'PROPERTY_TYPE_CODE'
    , 'ORIGINAL_PRINCIPAL_LIMIT'
    , 'MONTHLY_SERVICING_FEE_AMOUNT'
    , 'MORTGAGE_MARGIN'
    , 'HECM_LOAN_ORIGINAL_INTEREST_RATE'
    , 'RATE_RESET_FREQUENCY'
    , 'SERVICING_FEE_MARGIN'
    , 'BORROWER_1_AGE_AT_POOL_ISSUANCE'
    , 'BORROWER_2_AGE_AT_POOL_ISSUANCE'
    , 'LOAN_SERVICING_FEE_CODE'
    , 'PROPERTY_VALUATION_AMOUNT'
    , 'PROPERTY_VALUATION_EFFECTIVE_DATE'
    , 'LOAN_ORIGINATION_COMPANY'
    , 'HECM_LOAN_PURPOSE_CODE'
    , 'HECM_LOAN_PAYMENT_OPTION_CODE'
    , 'HECM_SAVER_FLAG'
    , 'ORIGINAL_DRAW_AMOUNT'
    , 'MORTGAGE_SERVICER'
    , 'LIFETIME_FLOOR_RATE'
    , 'INITIAL_CHANGE_DATE'
    , 'LIFETIME_INTEREST_RATE_CHANGE_CAP'
    , 'RESET_MONTHS'
    , 'ELIGIBLE_NON_BORROWING_SPOUSE'
    , 'ANNUAL_INTEREST_RATE_CHANGE_CAP'
    , 'MAXIMUM_INTEREST_RATE'
    , 'MANDATORY_PROPERTY_CHARGES_SET_ASIDE'
    , 'YOUNGEST_BORROWER_AGE_AT_POOL_ISSUANCE'
    , 'INITIAL_PLU'
    , 'EXPECTED_AVERAGE_MORTGAGE_INTEREST_RATE'
)

dirty_load_df.columns = new_columns
current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

dirty_load_df['CREATED_DATE'] = [current_time for x in range(0, len(dirty_load_df.index))]

sql_user = r'SELECT DATABASE_PRINCIPAL_ID()'
db_user = str(pd.read_sql(sql=sql_user, con=conn01).iloc[0, 0])

dirty_load_df['CREATED_USER'] = [db_user for x in range(0, len(dirty_load_df.index))]

cursor = conn01.cursor()
cursor.fast_executemany = True

columns = ", ".join(dirty_load_df.columns)

values = '(' + ', '.join(['?'] * len(dirty_load_df.columns)) + ')'

statement = "INSERT INTO " + table_name + " (" + columns + ") VALUES " + values

# extract values from DataFrame into list of tuples
insert = [tuple(x) for x in dirty_load_df.values]

cursor.executemany(statement, insert)
