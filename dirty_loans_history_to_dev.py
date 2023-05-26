import pandas as pd
import numpy as np
import datetime
import pyodbc
import sqlalchemy

source_file = r'C:\Users\jasonwalker\OneDrive - SitusAMC\Documents\project\baseline\source_file\202112\dirtyLoans12.08.20.csv'
server = r'COM-DLKDWD01.SITUSAMC.COM'
db = r'DW_LOAN_VALUATION'
dsn = r'DW_DEV_DW_LOAN_VALUATION'
target_table = r'DL'

conn01 = pyodbc.connect(
    "Driver={ODBC Driver 17 for SQL Server};"
    f"Server={server};"
    f"Database={db};"
    "UID=python;"
    "PWD=BPcpfhSx0t2669St2jDX;"
    , autocommit=True)

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

dirty_loan_df = pd.read_csv(filepath_or_buffer=source_file, delimiter=',', dtype=data_type)
dirty_loan_df['Payment Reason Code'] = dirty_loan_df['Payment Reason Code'].apply(lambda x: str(x).zfill(2))
dirty_loan_df['Age'] = dirty_loan_df['Age'].astype(int)
dirty_loan_df['Borrower 1 Age at Pool Issuance'] = dirty_loan_df['Borrower 1 Age at Pool Issuance'].astype(int)
dirty_loan_df['Borrower 2 Age at Pool Issuance'] = dirty_loan_df['Borrower 1 Age at Pool Issuance'].astype(int)
dirty_loan_df['Reset Months'] = dirty_loan_df['Reset Months'].replace(r'^\s*$', np.NaN, regex=True)
dirty_loan_df['Youngest Borrower Age at Pool Issuance'] = dirty_loan_df[
    'Youngest Borrower Age at Pool Issuance'].astype(int)
dirty_loan_df['HECM Original Funding Date'] = pd.to_datetime(dirty_loan_df['HECM Original Funding Date'],
                                                             errors='coerce')
dirty_loan_df['Property Valuation Effective Date'] = pd.to_datetime(dirty_loan_df['Property Valuation Effective Date'],
                                                                    errors='coerce')
dirty_loan_df['Initial Change Date'] = pd.to_datetime(dirty_loan_df['Initial Change Date'], errors='coerce')

new_columns = (
    'DisclosureSequenceNumber'
    , 'PaymentReasonCode'
    , 'HECMOriginalFundingDate'
    , 'OriginalHECMLoanBalance'
    , 'MIPBasisPoints'
    , 'MaximumClaimAmount'
    , 'OriginalAvailableLineOfCreditAmount'
    , 'Age'
    , 'PoolID'
    , 'IssuerID'
    , 'PropertyTypeCode'
    , 'OriginalPrincipalLimit'
    , 'MonthlyServicingFeeAmount'
    , 'MortgageMargin'
    , 'HECMLoanOriginalInterestRate'
    , 'RateResetFrequency'
    , 'ServicingFeeMargin'
    , 'Borrower1AgeAtPoolIssuance'
    , 'Borrower2AgeAtPoolIssuance'
    , 'LoanServicingFeeCode'
    , 'PropertyValuationAmount'
    , 'PropertyValuationEffectiveDate'
    , 'LoanOriginationCompany'
    , 'HECMLoanPurposeCode'
    , 'HECMLoanPaymentOptionCode'
    , 'HECMSaverFlag'
    , 'OriginalDrawAmount'
    , 'MortgageServicer'
    , 'LifetimeFloorRate'
    , 'InitialChangeDate'
    , 'LifetimeInterestRateChangeCap'
    , 'ResetMonths'
    , 'EligibleNonBorrowingSpouse'
    , 'AnnualInterestRateChangeCap'
    , 'MaximumInterestRate'
    , 'MandatoryPropertyChargesSetAside'
    , 'YoungestBorrowerAgeAtPoolIssuance'
    , 'InitialPLU'
    , 'ExpectedAverageMortgageInterestRate'
)

dirty_loan_df.columns = new_columns

# load dw table with sqlalchemy engine
engine = sqlalchemy.create_engine(
        f"mssql+pyodbc://@{dsn}",
        fast_executemany=True)

conn = engine.connect()

dirty_loan_df.to_sql(target_table, conn, if_exists='append', index=False)
print('success')


