import requests
from bs4 import BeautifulSoup as bs
from urllib.parse import urljoin
import datetime
from dateutil.relativedelta import relativedelta
import sys
import pandas as pd
import numpy as np
import openpyxl
import xlrd
import pyodbc
import sqlalchemy
import os


# python libraries included in DEV
# pandas, numpy, requests, urllib, dateutil, openpyxl, xlrd, pyodbc, sqlalchemy

# python libraries missing in DEV
# bs4,



# variable
url = r'https://www.hud.gov/program_offices/housing/rmra/oe/rpts/hecmsfsnap/hecmsfsnap'
excel_filter = r'.xls'
local_file = r'c:\temp\hecm_endorsement.XLSX'
date_mode = 'a'  # a = automatic, m = manual with file_year and file_month
target_schema = 'stage'
src_stage_table = 'SRC_HECM_ENDORSEMENT'
stage_table = 'HECM_ENDORSEMENT'
source_name = 'U.S. DEPARTMENT OF HOUSING AND URBAN DEVELOPMENT'
source_name_state = 'DATA WAREHOUSE STANDARD DIMENSION'
country_name_usa = 'UNITED STATES'
month_look_back = 2
file_year = 2022
file_month = 9

# environment: (server, driver, uname, pword, dsn)
env_info = {'dev': (r'COM-DLKDWD01.SITUSAMC.COM', r'ODBC Driver 17 for SQL Server', r'python', r'FZ9H2Pcg=z%-cf?$', r'DW_DEV_DW_LOAN_VALUATION')
    , 'uat': (r'COM-DWDBU01.SITUSAMC.COM', r'ODBC Driver 17 for SQL Server', r'python' ,r'FZ9H2Pcg=z%-cf?$', r'DW_UAT_DW_LOAN_VALUATION')
    , 'prod': (r'COM-DWAGP01.SITUSAMC.COM', r'SQL Server', r'python', r'FZ9H2Pcg=z%-cf?$', r'DW_PROD_DW_LOAN_VALUATION')
     }

env = env_info['prod']
db = r'DW_LOAN_VALUATION'

server = env[0]
driver = env[1]
uname = env[2]
pword = env[3]
dsn = env[4]


# column mapping
column_mapping = {
    'endorsement_year': 'ENDORSEMENT_YEAR'
    , 'endorsement_month': 'ENDORSEMENT_MONTH'
    , 'property_state': 'PROPERTY_STATE_CODE'
    , 'property_county': 'PROPERTY_COUNTY'
    , 'property_city': 'PROPERTY_CITY'
    , 'property_zip': 'PROPERTY_ZIP_CODE'
    , 'originating_mortgagee': 'ORIGINATING_MORTGAGEE_SPONSOR_ORIGINATOR'
    , 'originating_mortgagee_number': 'ORIGINATING_MORTGAGEE_NUMBER'
    , 'sponsor_name': 'SPONSOR_NAME'
    , 'sponsor_number': 'SPONSOR_NUMBER'
    , 'Sponsor_Number': 'SPONSOR_NUMBER'
    , 'refinance': 'PURCHASE_REFINANCE'
    , 'rate_type': 'RATE_TYPE'
    , 'interest_rate': 'INTEREST_RATE'
    , 'initial_principal_limit': 'INITIAL_PRINCIPAL_LIMIT'
    , 'maximum_claim_amount': 'MAXIMUM_CLAIM_AMOUNT'
    , 'Property State': 'PROPERTY_STATE_CODE'
    , 'Property City': 'PROPERTY_CITY'
    , 'Property County': 'PROPERTY_COUNTY'
    , 'Property Zip': 'PROPERTY_ZIP_CODE'
    , 'Originating Mortgagee': 'ORIGINATING_MORTGAGEE_SPONSOR_ORIGINATOR'
    , 'Originating Mortgagee Number': 'ORIGINATING_MORTGAGEE_NUMBER'
    , 'Sponsor Name': 'SPONSOR_NAME'
    , 'Sponosr Number': 'SPONSOR_NUMBER'
    , 'RefinanceType*': 'PURCHASE_REFINANCE'
    , 'Rate Type': 'RATE_TYPE'
    , 'Interest Rate': 'INTEREST_RATE'
    , 'Initial Principal Limit': 'INITIAL_PRINCIPAL_LIMIT'
    , 'Maximum Claim Amount': 'MAXIMUM_CLAIM_AMOUNT'
    , 'Endorsement Year': 'ENDORSEMENT_YEAR'
    , 'Endorsement Month': 'ENDORSEMENT_MONTH'
    , 'RefinanceType': 'PURCHASE_REFINANCE'
    , 'Sponsored Originator': 'SPONSOR_ORIGINATOR'
    , 'NMLS*': 'NMLS_ID'
    , 'Standard/Saver': 'STANDARD_SAVER'
    , 'Purchase /Refinance': 'PURCHASE_REFINANCE'
    , 'Hecm Type': 'HECM_TYPE'
    , 'Originating Mortgagee Sponsor Originator': 'ORIGINATING_MORTGAGEE_SPONSOR_ORIGINATOR'
    , 'Sponsor Number': 'SPONSOR_NUMBER'
    , 'Sponsor Originator': 'SPONSOR_ORIGINATOR'
    , 'Current Servicer ID': 'CURRENT_SERVICER_ID'
    , 'Previous Servicer': 'PREVIOUS_SERVICER'
    , 'NMLS': 'NMLS_ID'
    , 'Standard Saver': 'STANDARD_SAVER'
    , 'Purchase Refinance': 'PURCHASE_REFINANCE'
    , 'Originating_Mortgagee_Sponsor_Or': 'ORIGINATING_MORTGAGEE_SPONSOR_ORIGINATOR'
    , 'Originating_Mortgagee_Number': 'ORIGINATING_MORTGAGEE_NUMBER'
    , 'Sponsor_Name': 'SPONSOR_NAME'
    , 'Sponsor_Originator': 'SPONSOR_ORIGINATOR'
    , 'Current_Servicer_ID': 'CURRENT_SERVICER_ID'
    , 'Previous_Servicer': 'PREVIOUS_SERVICER'
    , 'Hecm_Type': 'HECM_TYPE'
    , 'Rate_Type': 'RATE_TYPE'
    , 'Interest_Rate': 'INTEREST_RATE'
    , 'Initial_Principal_Limit': 'INITIAL_PRINCIPAL_LIMIT'
    , 'Maximum_Claim_Amount': 'MAXIMUM_CLAIM_AMOUNT'
    , 'Endorsement_Year': 'ENDORSEMENT_YEAR'
    , 'Endorsement_Month': 'ENDORSEMENT_MONTH'
    , 'Standard_Saver': 'STANDARD_SAVER'
    , 'Purchase_Refinance': 'PURCHASE_REFINANCE'
    , 'Originating Mortgagee/Sponsor Originator': 'ORIGINATING_MORTGAGEE_SPONSOR_ORIGINATOR'
    , 'Previous Servicer ID': 'PREVIOUS_SERVICER'
    , 'Refinance Type*': 'PURCHASE_REFINANCE'
    , 'Purchase/Refinance': 'PURCHASE_REFINANCE'
}


column_order = [
    'PROPERTY_STATE_CODE'
    , 'PROPERTY_COUNTY'
    , 'PROPERTY_CITY'
    , 'PROPERTY_ZIP_CODE'
    , 'ORIGINATING_MORTGAGEE_SPONSOR_ORIGINATOR'
    , 'ORIGINATING_MORTGAGEE_NUMBER'
    , 'SPONSOR_NAME'
    , 'SPONSOR_NUMBER'
    , 'SPONSOR_ORIGINATOR'
    , 'CURRENT_SERVICER_ID'
    , 'PREVIOUS_SERVICER'
    , 'NMLS_ID'
    , 'STANDARD_SAVER'
    , 'PURCHASE_REFINANCE'
    , 'HECM_TYPE'
    , 'RATE_TYPE'
    , 'INTEREST_RATE'
    , 'INITIAL_PRINCIPAL_LIMIT'
    , 'MAXIMUM_CLAIM_AMOUNT'
    , 'ENDORSEMENT_YEAR'
    , 'ENDORSEMENT_MONTH'
]


# function
def get_excel_file_paths(url):
    page = requests.get(url).text
    soup = bs(page, 'html.parser')
    path_return = [a['href'] for a in soup.select('.genlink li a')
                   if excel_filter in a['href']]
    return path_return

def insert_data(schema
                , table
                , skip_column_position=[]  # list of column positions to skip starting at position 1
                ):
    if len(skip_column_position) > 0:
        skip_column_string = 'AND ORDINAL_POSITION NOT IN (' + ', '.join(str(s) for s in skip_column_position) + ')'
    else:
        skip_column_string = ''

    sql_column = f"""
    SELECT
        COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE
        TABLE_SCHEMA = '{schema}'
        AND TABLE_NAME = '{table}'
        {skip_column_string}
    ORDER BY ORDINAL_POSITION
    """

    target_columns = [x[0] for x in pd.read_sql(sql_column, con=conn01).values.tolist()]
    insert_column = ', '.join(target_columns)
    value_placeholder = ', '.join('?' for x in target_columns)

    return insert_column, value_placeholder


# main program
if date_mode == 'a':
    # automatic date logic, get file two months ago
    today = datetime.date.today()
    two_month_ago_date = today - relativedelta(months=month_look_back)

    #set endorsment date variables for automatic date logic
    file_year = two_month_ago_date.year
    file_month = two_month_ago_date.month

    two_month_ago_file_year = datetime.datetime.strftime(two_month_ago_date, '%Y')
    two_month_ago_month_abr = datetime.datetime.strftime(two_month_ago_date, '%b')
    two_month_ago_month_full = datetime.datetime.strftime(two_month_ago_date, '%B')
    two_month_ago_month_first_4 = two_month_ago_month_full[:4]
    file_search_full = two_month_ago_month_full + two_month_ago_file_year
    file_search_abbr = two_month_ago_month_abr + two_month_ago_file_year
    file_search_first_4 = two_month_ago_month_first_4 + two_month_ago_file_year
else:
    # manual date logic, use variables in script
    date_month = datetime.datetime.strptime(str(file_month), '%m')
    date_full = date_month.strftime('%B')
    date_abbr = date_month.strftime('%b')
    month_first_4 = date_full[:4]
    file_search_full = date_full + str(file_year)
    file_search_abbr = date_abbr + str(file_year)
    file_search_first_4 = month_first_4 + str(file_year)

# get excel files from url
excel_file_paths = get_excel_file_paths(url)

# check for excel file based on month abbreviation or full month name
file_full_date = [f for f in excel_file_paths if file_search_full in f]
file_abbr_date = [f for f in excel_file_paths if file_search_abbr in f]
file_first_4_date = [f for f in excel_file_paths if file_search_first_4 in f]

if len(file_abbr_date) > 0:
    download_file = urljoin(url, file_abbr_date[0])

if len(file_full_date) > 0:
    download_file = urljoin(url, file_full_date[0])

if len(file_first_4_date) > 0:
    download_file = urljoin(url, file_first_4_date[0])
try:
    print(download_file)
except:
    print('no file available, process cancelled')
    sys.exit()

# rename to xls if source file is xls
if download_file.upper().endswith('.XLS'):
    local_file = local_file.replace('.XLSX', '.XLS')

# set month excel file to download
url_file = requests.get(download_file)

# save file
with open(local_file, 'wb') as f:
    f.write(url_file.content)

if local_file.endswith('.XLSX'):
    wb = openpyxl.load_workbook(local_file)
    sheet_names = wb.sheetnames
    wb.close()
elif local_file.endswith('.XLS'):
    wb = xlrd.open_workbook(filename=local_file, on_demand=True)
    sheet_names = [s for s in wb.sheet_names()]
    wb.release_resources()
    del wb
else:
    print('excel file type not coded, process cancelled')
    sys.exit()

source_sheet = [s for s in sheet_names if
                (s.startswith('Hecm Data')) or
                (s.startswith('Hecm data')) or
                (s.startswith('HecmD')) or
                (s.startswith('HECM DATA')) or
                (s.startswith('HECM Data')) or
                (s.startswith('Data Hecm')) or
                (s.startswith('HECMD'))
                ][0]

# does sheet name exist
if len(source_sheet) == 0:
    print('sheet name starting with "Hecm Data" does not exist, process cancelled')
    sys.exit()

# read file into dataframe
df_source = pd.read_excel(io=local_file, sheet_name=source_sheet)

in_column = df_source.columns.values.tolist()

# test source file columns with column_mapping dictionary
unmapped_column = [c for c in in_column if c not in column_mapping]

if len(unmapped_column) > 0:
    print('columns (' + ', '.join(unmapped_column) + ') are unmapped in column_mapping, process cancelled')
    sys.exit()

df_data = df_source.rename(columns=column_mapping)

# column sort order for columns that exist in source data
column_order_exists = [c for c in column_order if c in df_data.columns]

# replace empty string with np.nan
df_data = df_data.replace(r'^\s+$', np.nan, regex=True)

# replace 'Not Available' with np.nan
df_data = df_data.replace('Not Available', np.nan, regex=True)
df_data = df_data.replace('NOT AVAILABLE', np.nan, regex=True)

if 'NMLS_ID' in df_data.columns:
    df_data['NMLS_ID'] = df_data['NMLS_ID'].apply(lambda x: np.nan if len(str(x)) == 0 else x)
    df_data['NMLS_ID'] = df_data['NMLS_ID'].astype('float')
    df_data['NMLS_ID'] = df_data['NMLS_ID'].astype('Int64')

df_data['SPONSOR_NUMBER'] = df_data['SPONSOR_NUMBER'].apply(lambda x: np.nan if len(str(x)) == 0 else x)
df_data['SPONSOR_NUMBER'] = df_data['SPONSOR_NUMBER'].astype('float')
df_data['SPONSOR_NUMBER'] = df_data['SPONSOR_NUMBER'].astype('Int64')

if 'ORIGINATING_MORTGAGEE_NUMBER' in df_data.columns:
    df_data['ORIGINATING_MORTGAGEE_NUMBER'] = df_data['ORIGINATING_MORTGAGEE_NUMBER'].apply(
        lambda x: np.nan if len(str(x)) == 0 else x)
    df_data['ORIGINATING_MORTGAGEE_NUMBER'] = df_data['ORIGINATING_MORTGAGEE_NUMBER'].astype('float')
    df_data['ORIGINATING_MORTGAGEE_NUMBER'] = df_data['ORIGINATING_MORTGAGEE_NUMBER'].astype('Int64')

if 'CURRENT_SERVICER_ID' in df_data.columns:
    df_data['CURRENT_SERVICER_ID'] = df_data['CURRENT_SERVICER_ID'].apply(
        lambda x: str(x).split('.')[0] if str(x).find('.') >= 0 else x)
    df_data['CURRENT_SERVICER_ID'] = df_data['CURRENT_SERVICER_ID'].astype('float')
    df_data['CURRENT_SERVICER_ID'] = df_data['CURRENT_SERVICER_ID'].astype('Int64')

if 'PREVIOUS_SERVICER' in df_data.columns:
    df_data['PREVIOUS_SERVICER'] = df_data['PREVIOUS_SERVICER'].apply(
        lambda x: str(x).split('.')[0] if str(x).find('.') >= 0 else x)
    df_data['PREVIOUS_SERVICER'] = df_data['PREVIOUS_SERVICER'].astype('float')
    df_data['PREVIOUS_SERVICER'] = df_data['PREVIOUS_SERVICER'].astype('Int64')

df_data['PROPERTY_STATE_CODE'] = df_data['PROPERTY_STATE_CODE'].str.strip().str.upper()
df_data['PROPERTY_COUNTY'] = df_data['PROPERTY_COUNTY'].str.strip().str.upper()
df_data['PROPERTY_CITY'] = df_data['PROPERTY_CITY'].str.strip().str.upper()

if 'ORIGINATING_MORTGAGEE_SPONSOR_ORIGINATOR' in df_data.columns:
    df_data['ORIGINATING_MORTGAGEE_SPONSOR_ORIGINATOR'] = \
        df_data['ORIGINATING_MORTGAGEE_SPONSOR_ORIGINATOR'].apply(
            lambda x: np.nan if len(str(x)) == 0 else x)
    df_data['ORIGINATING_MORTGAGEE_SPONSOR_ORIGINATOR'] = \
        df_data['ORIGINATING_MORTGAGEE_SPONSOR_ORIGINATOR'].str.strip().str.upper()
if 'SPONSOR_NAME' in df_data.columns:
    df_data['SPONSOR_NAME'] = df_data['SPONSOR_NAME'].apply(
        lambda x: np.nan if len(str(x)) == 0 else x)
    df_data['SPONSOR_NAME'] = df_data['SPONSOR_NAME'].str.strip().str.upper()

if 'SPONSOR_ORIGINATOR' in df_data.columns:
    df_data['SPONSOR_ORIGINATOR'] = df_data['SPONSOR_ORIGINATOR'].apply(
        lambda x: np.nan if len(str(x)) == 0 else x)
    df_data['SPONSOR_ORIGINATOR'] = df_data['SPONSOR_ORIGINATOR'].str.strip().str.upper()
if 'STANDARD_SAVER' in df_data.columns:
    df_data['STANDARD_SAVER'] = df_data['STANDARD_SAVER'].str.strip().str.upper()
if 'PURCHASE_REFINANCE' in df_data.columns:
    df_data['PURCHASE_REFINANCE'] = df_data['PURCHASE_REFINANCE'].str.strip().str.upper()
if 'HECM_TYPE' in df_data.columns:
    df_data['HECM_TYPE'] = df_data['HECM_TYPE'].str.strip().str.upper()
if 'RATE_TYPE' in df_data.columns:
    df_data['RATE_TYPE'] = df_data['RATE_TYPE'].str.strip().str.upper()

df_data.dropna(axis=0, how='any', subset=['ENDORSEMENT_YEAR', 'ENDORSEMENT_MONTH'], inplace=True)

df_src_stage = df_data[column_order_exists].copy()

conn01 = pyodbc.connect(
    f"Driver={driver};"
    f"Server={server};"
    f"Database={db};"
    f"UID={uname};"
    f"PWD={pword};"
    , autocommit=True)

sql_data_source = f"""
SELECT SOURCE_SID FROM DW_DIMENSIONS.dbo.MLU_DATA_SOURCE WHERE SOURCE_NAME = '{source_name}'
"""

data_source = pd.read_sql(sql=sql_data_source, con=conn01).iloc[0, 0]
created_user = pd.read_sql(sql=r'SELECT DATABASE_PRINCIPAL_ID()', con=conn01).iloc[0, 0]
time_stamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
df_src_stage['DATA_SOURCE'] = [data_source for x in range(len(df_src_stage.index))]
df_src_stage['DATA_SOURCE'] = df_src_stage['DATA_SOURCE'].astype(str)
df_src_stage['CREATED_DATE'] = [time_stamp for x in range(len(df_src_stage.index))]
df_src_stage['CREATED_DATE'] = df_src_stage['CREATED_DATE'].astype(str)
df_src_stage['CREATED_USER'] = [created_user for x in range(len(df_src_stage.index))]
df_src_stage['CREATED_USER'] = df_src_stage['CREATED_USER'].astype(np.int64)

sql_data_source_state = f"""
SELECT SOURCE_SID FROM DW_DIMENSIONS.dbo.MLU_DATA_SOURCE WHERE SOURCE_NAME = '{source_name_state}'
"""

data_source_state = pd.read_sql(sql=sql_data_source_state, con=conn01).iloc[0, 0]
data_source_county = data_source_state
data_source_city = data_source_state

sql_country_key_usa = f"""
SELECT COUNTRY_KEY FROM DW_DIMENSIONS.dbo.MLU_COUNTRY WHERE COUNTRY_NAME = '{country_name_usa}'
"""

country_key_usa = pd.read_sql(sql=sql_country_key_usa, con=conn01).iloc[0, 0]

with conn01.cursor() as cursor:
    # TRUNCATE SOURCE STAGE TABLE
    cursor.execute(f'TRUNCATE TABLE {target_schema}.{src_stage_table}')

    # INSERT SRC_STAGE TABLE FROM DATAFRAME
    engine = sqlalchemy.create_engine(
        f"mssql+pyodbc://@{dsn}",
        fast_executemany=True)

    df_src_stage.to_sql(name=src_stage_table, con=engine, schema=f'{target_schema}', if_exists='append', index=False)

    # filtered delete from stage table on endorsement date
    sql_delete = f"""
    DELETE FROM {target_schema}.{stage_table} 
    WHERE 
        ENDORSEMENT_YEAR = {file_year} 
        AND ENDORSEMENT_MONTH = {file_month}
    ;
    """
    cursor.execute(sql_delete)

    #insert into stage table from src stage table
    insert_sql = f"""
    DECLARE @DATA_SOURCE_STATE BIGINT = (SELECT SOURCE_SID	FROM DW_DIMENSIONS.dbo.MLU_DATA_SOURCE WHERE SOURCE_NAME = '{source_name_state}');
    DECLARE @COUNTRY_KEY_USA BIGINT = (SELECT COUNTRY_KEY FROM DW_DIMENSIONS.dbo.MLU_COUNTRY	WHERE COUNTRY_NAME = '{country_name_usa}') 
    DECLARE @DATA_SOURCE_COUNTY BIGINT = @DATA_SOURCE_STATE
    DECLARE @DATA_SOURCE_CITY BIGINT = @DATA_SOURCE_STATE
    
    DECLARE @DATA_SOURCE_HUD BIGINT = (
        SELECT 
            SOURCE_SID
        FROM DW_DIMENSIONS.dbo.MLU_DATA_SOURCE WITH(NOLOCK)
        WHERE
            SOURCE_NAME = '{source_name}'
    )
    
    
    DROP TABLE IF EXISTS #ENDORSEMENT_1;
    DROP TABLE IF EXISTS #ENDORSEMENT_2;
    DROP TABLE IF EXISTS #ENDORSEMENT_3;
    DROP TABLE IF EXISTS #STATE_KEY;
    DROP TABLE IF EXISTS #COUNTY_KEY;
    DROP TABLE IF EXISTS #CITY_KEY;
    DROP TABLE IF EXISTS #ISSUER_KEY;
    DROP TABLE IF EXISTS #ORIGINATOR_KEY;
    DROP TABLE IF EXISTS #PURCHASE_REFINANCE_KEY;
    DROP TABLE IF EXISTS #HECM_TYPE_KEY;
    DROP TABLE IF EXISTS #RATE_TYPE_KEY;
    
    
    SELECT * INTO #ENDORSEMENT_1 FROM {target_schema}.{stage_table} WHERE 1 = 0;
    SELECT * INTO #ENDORSEMENT_2 FROM {target_schema}.{stage_table} WHERE 1 = 0;
    SELECT * INTO #ENDORSEMENT_3 FROM {target_schema}.{stage_table} WHERE 1 = 0;
    
    
    INSERT INTO #ENDORSEMENT_1 (
    PROPERTY_STATE_CODE,
    STATE_CODE,
    PROPERTY_COUNTY,
    PROPERTY_CITY,
    PROPERTY_ZIP_CODE,
    ORIGINATING_MORTGAGEE_SPONSOR_ORIGINATOR,
    ISSUER,
    ORIGINATING_MORTGAGEE_NUMBER,
    SPONSOR_NAME,
    SPONSOR_NUMBER,
    SPONSOR_ORIGINATOR,
    CURRENT_SERVICER_ID,
    PREVIOUS_SERVICER,
    NMLS_ID,
    STANDARD_SAVER,
    PURCHASE_REFINANCE,
    HECM_TYPE,
    RATE_TYPE,
    INTEREST_RATE,
    INITIAL_PRINCIPAL_LIMIT,
    MAXIMUM_CLAIM_AMOUNT,
    ENDORSEMENT_YEAR,
    ENDORSEMENT_MONTH,
    [DATA_SOURCE]
    )
    SELECT
        E.PROPERTY_STATE_CODE,
        COALESCE(E.PROPERTY_STATE_CODE, BN_O.STATE_CODE, BN_SO.STATE_CODE) AS STATE_CODE,
        E.PROPERTY_COUNTY,
        E.PROPERTY_CITY,
        CASE
            WHEN CHARINDEX('.', E.PROPERTY_ZIP_CODE) > 0 THEN
                RIGHT('00000' + LEFT(E.PROPERTY_ZIP_CODE, CHARINDEX('.', E.PROPERTY_ZIP_CODE) - 1), 5)
            ELSE RIGHT('00000' + E.PROPERTY_ZIP_CODE, 5)
            END AS PROPERTY_ZIP_CODE,
        E.ORIGINATING_MORTGAGEE_SPONSOR_ORIGINATOR,
        CASE
            WHEN E.SPONSOR_NAME IS NULL
                THEN UPPER(TRIM(E.ORIGINATING_MORTGAGEE_SPONSOR_ORIGINATOR))
            ELSE UPPER(TRIM(E.SPONSOR_NAME))
            END AS ISSUER,	
        E.ORIGINATING_MORTGAGEE_NUMBER,
        E.SPONSOR_NAME,
        E.SPONSOR_NUMBER,
        E.SPONSOR_ORIGINATOR,
        E.CURRENT_SERVICER_ID,
        E.PREVIOUS_SERVICER,
        E.NMLS_ID,
        E.STANDARD_SAVER,
        E.PURCHASE_REFINANCE,
        E.HECM_TYPE,
        E.RATE_TYPE,
        E.INTEREST_RATE,
        E.INITIAL_PRINCIPAL_LIMIT,
        E.MAXIMUM_CLAIM_AMOUNT,
        E.ENDORSEMENT_YEAR,
        E.ENDORSEMENT_MONTH,
        E.DATA_SOURCE
    FROM {target_schema}.{src_stage_table} E WITH(NOLOCK)
    LEFT JOIN [stage].[SRC_BROKER_NMLS] BN_O WITH(NOLOCK)
        ON E.ORIGINATING_MORTGAGEE_SPONSOR_ORIGINATOR = BN_O.BROKER_NAME
    LEFT JOIN [stage].[SRC_BROKER_NMLS] BN_SO WITH(NOLOCK)
        ON E.SPONSOR_ORIGINATOR = BN_SO.BROKER_NAME
        AND E.SPONSOR_NAME IS NOT NULL
    ;
    
    
    /*STATE KEY*/
    CREATE TABLE #STATE_KEY (
    STATE_CODE VARCHAR(10) PRIMARY KEY
    ,STATE_KEY BIGINT
    );
    
    INSERT INTO #STATE_KEY (
    STATE_CODE
    )
    SELECT DISTINCT
        STATE_CODE
    FROM #ENDORSEMENT_1
    ;
    
    UPDATE #STATE_KEY
    SET STATE_KEY = DW_DIMENSIONS.stage.fn_DIM_KEY(
                        'STATE'
                        ,@DATA_SOURCE_STATE
                        ,NULL
                        ,UPPER(TRIM(STATE_CODE))
                        ,NULL
                        ,@COUNTRY_KEY_USA
                        ,NULL
                        )
    ;
    
    CREATE NONCLUSTERED INDEX IDX_STATE_CODE ON #ENDORSEMENT_1 (STATE_CODE);
    
    
    
    /*PURCHASE_REFINANCE_KEY*/
    CREATE TABLE #PURCHASE_REFINANCE_KEY (
    [PURCHASE_REFINANCE] VARCHAR(25) PRIMARY KEY
    ,[PURCHASE_REFINANCE_KEY] BIGINT
    );
    
    INSERT INTO #PURCHASE_REFINANCE_KEY (
    [PURCHASE_REFINANCE]
    )
    SELECT DISTINCT
        [PURCHASE_REFINANCE]
    FROM #ENDORSEMENT_1
    WHERE 
        NULLIF([PURCHASE_REFINANCE], '') IS NOT NULL
    ;
    
    UPDATE #PURCHASE_REFINANCE_KEY
    SET PURCHASE_REFINANCE_KEY = DW_DIMENSIONS.stage.fn_DIM_KEY(
                                    'HECM_PURCHASE_REFINANCE'
                                    ,@DATA_SOURCE_HUD
                                    ,NULL
                                    ,NULL
                                    ,[PURCHASE_REFINANCE]
                                    ,NULL
                                    ,NULL
                                    )
    ;
    
    
    CREATE NONCLUSTERED INDEX IDX_PURCHASE_REFINANCE ON #ENDORSEMENT_1 (PURCHASE_REFINANCE);
    
    
    /*HECM_TYPE_KEY*/
    CREATE TABLE #HECM_TYPE_KEY (
    [HECM_TYPE] VARCHAR(50) PRIMARY KEY
    ,[HECM_TYPE_KEY] BIGINT
    );
    
    INSERT INTO #HECM_TYPE_KEY (
    [HECM_TYPE]
    )
    SELECT DISTINCT
        [HECM_TYPE]
    FROM #ENDORSEMENT_1
    WHERE
        NULLIF([HECM_TYPE], '') IS NOT NULL
    ;
    
    UPDATE #HECM_TYPE_KEY
    SET HECM_TYPE_KEY = DW_DIMENSIONS.stage.fn_DIM_KEY(
                            'HECM_TYPE'
                            ,@DATA_SOURCE_HUD
                            ,NULL
                            ,NULL
                            ,[HECM_TYPE]
                            ,NULL
                            ,NULL
                            )
    ;
    
    
    CREATE NONCLUSTERED INDEX IDX_HECM_TYPE ON #ENDORSEMENT_1 (HECM_TYPE);
    
    
    
    /*RATE_TYPE_KEY*/
    CREATE TABLE #RATE_TYPE_KEY (
    [RATE_TYPE] VARCHAR(25) PRIMARY KEY
    ,[RATE_TYPE_KEY] BIGINT
    );
    
    INSERT INTO #RATE_TYPE_KEY (
    [RATE_TYPE]
    )
    SELECT DISTINCT
        [RATE_TYPE]
    FROM #ENDORSEMENT_1
    WHERE
        NULLIF([RATE_TYPE], '') IS NOT NULL
    ;
    
    UPDATE #RATE_TYPE_KEY
    SET RATE_TYPE_KEY = DW_DIMENSIONS.stage.fn_DIM_KEY(
                            'HECM_RATE_TYPE'
                            ,@DATA_SOURCE_HUD
                            ,NULL
                            ,NULL
                            ,[RATE_TYPE]
                            ,NULL
                            ,NULL
                            )
    ;
    
    
    CREATE NONCLUSTERED INDEX IDX_RATE_TYPE ON #ENDORSEMENT_1 (RATE_TYPE);
    
    
    
    
    /*ORIGINATOR*/
    CREATE TABLE #ORIGINATOR_KEY (
    [ORIGINATOR] VARCHAR(255) PRIMARY KEY
    ,[ORIGINATOR_KEY] BIGINT
    );
    
    INSERT INTO #ORIGINATOR_KEY (
    [ORIGINATOR]
    )
    SELECT DISTINCT
        [ORIGINATING_MORTGAGEE_SPONSOR_ORIGINATOR]
    FROM #ENDORSEMENT_1
    WHERE
        NULLIF([ORIGINATING_MORTGAGEE_SPONSOR_ORIGINATOR], '') IS NOT NULL
    ;
    
    UPDATE #ORIGINATOR_KEY
    SET ORIGINATOR_KEY = DW_DIMENSIONS.stage.fn_CLIENT_KEY(
                            'ORIGINATOR'
                            ,@DATA_SOURCE_HUD
                            ,NULL
                            ,NULL
                            ,UPPER(TRIM([ORIGINATOR]))
                            )
    ;
    
    
    CREATE NONCLUSTERED INDEX IDX_ORIGINATING_MORTGAGEE_SPONSOR_ORIGINATOR ON #ENDORSEMENT_1 (ORIGINATING_MORTGAGEE_SPONSOR_ORIGINATOR);
    
    
    
    
    
    
    /*ISSUER*/
    CREATE TABLE #ISSUER_KEY (
    [ISSUER] VARCHAR(255) PRIMARY KEY
    ,[ISSUER_KEY] BIGINT
    );
    
    INSERT INTO #ISSUER_KEY (
    [ISSUER]
    )
    SELECT DISTINCT
        [ISSUER]
    FROM #ENDORSEMENT_1
    WHERE
        NULLIF([ISSUER], '') IS NOT NULL
    ;
    
    UPDATE #ISSUER_KEY
    SET ISSUER_KEY = DW_DIMENSIONS.stage.fn_CLIENT_KEY(
                            'ISSUER'
                            ,@DATA_SOURCE_HUD
                            ,NULL
                            ,NULL
                            ,UPPER(TRIM([ISSUER]))
                            )
    ;
    
    
    CREATE NONCLUSTERED INDEX IDX_ISSUER ON #ENDORSEMENT_1 (ISSUER);
    
    
    
    
    INSERT INTO #ENDORSEMENT_2 (
    [PROPERTY_STATE_CODE]
    ,[STATE_KEY]
    ,[STATE_CODE]
    ,[PROPERTY_COUNTY]
    ,[PROPERTY_CITY]
    ,[PROPERTY_ZIP_CODE]
    ,[ORIGINATOR_KEY]
    ,[ORIGINATING_MORTGAGEE_SPONSOR_ORIGINATOR]
    ,[ISSUER_KEY]
    ,[ISSUER]
    ,[ORIGINATING_MORTGAGEE_NUMBER]
    ,[SPONSOR_NAME]
    ,[SPONSOR_NUMBER]
    ,[SPONSOR_ORIGINATOR]
    ,[CURRENT_SERVICER_ID]
    ,[PREVIOUS_SERVICER]
    ,[NMLS_ID]
    ,[STANDARD_SAVER]
    ,[PURCHASE_REFINANCE_KEY]
    ,[PURCHASE_REFINANCE]
    ,[HECM_TYPE_KEY]
    ,[HECM_TYPE]
    ,[RATE_TYPE_KEY]
    ,[RATE_TYPE]
    ,[INTEREST_RATE]
    ,[INITIAL_PRINCIPAL_LIMIT]
    ,[MAXIMUM_CLAIM_AMOUNT]
    ,[ENDORSEMENT_YEAR]
    ,[ENDORSEMENT_MONTH]
    ,[DATA_SOURCE]
    )
    SELECT
        E.[PROPERTY_STATE_CODE]
        ,S.[STATE_KEY]
        ,E.[STATE_CODE]
        ,E.[PROPERTY_COUNTY]
        ,E.[PROPERTY_CITY]
        ,E.[PROPERTY_ZIP_CODE]
        ,O.[ORIGINATOR_KEY]
        ,E.[ORIGINATING_MORTGAGEE_SPONSOR_ORIGINATOR]
        ,I.[ISSUER_KEY]
        ,E.[ISSUER]
        ,E.[ORIGINATING_MORTGAGEE_NUMBER]
        ,E.[SPONSOR_NAME]
        ,E.[SPONSOR_NUMBER]
        ,E.[SPONSOR_ORIGINATOR]
        ,E.[CURRENT_SERVICER_ID]
        ,E.[PREVIOUS_SERVICER]
        ,E.[NMLS_ID]
        ,E.[STANDARD_SAVER]
        ,PR.[PURCHASE_REFINANCE_KEY]
        ,E.[PURCHASE_REFINANCE]
        ,HT.[HECM_TYPE_KEY]
        ,E.[HECM_TYPE]
        ,RT.[RATE_TYPE_KEY]
        ,E.[RATE_TYPE]
        ,E.[INTEREST_RATE]
        ,E.[INITIAL_PRINCIPAL_LIMIT]
        ,E.[MAXIMUM_CLAIM_AMOUNT]
        ,E.[ENDORSEMENT_YEAR]
        ,E.[ENDORSEMENT_MONTH]
        ,E.[DATA_SOURCE]
    FROM #ENDORSEMENT_1 E
    LEFT JOIN #STATE_KEY S
        ON E.STATE_CODE = S.STATE_CODE
    LEFT JOIN #ORIGINATOR_KEY O
        ON E.ORIGINATING_MORTGAGEE_SPONSOR_ORIGINATOR = O.ORIGINATOR
    LEFT JOIN #ISSUER_KEY I
        ON E.ISSUER = I.ISSUER
    LEFT JOIN #PURCHASE_REFINANCE_KEY PR
        ON E.PURCHASE_REFINANCE = PR.PURCHASE_REFINANCE
    LEFT JOIN #HECM_TYPE_KEY HT
        ON E.HECM_TYPE = HT.HECM_TYPE
    LEFT JOIN #RATE_TYPE_KEY RT
        ON E.RATE_TYPE = RT.RATE_TYPE	
    ;
    
    
    
    
    /*COUNTY*/
    CREATE TABLE #COUNTY_KEY (
    STATE_KEY BIGINT
    ,COUNTY VARCHAR(100)
    ,COUNTY_KEY BIGINT
    ,CONSTRAINT PK_STATE_COUNTY PRIMARY KEY (STATE_KEY, COUNTY)
    );
    
    INSERT INTO #COUNTY_KEY (
    STATE_KEY
    ,COUNTY
    )
    SELECT DISTINCT
        STATE_KEY
        ,UPPER(TRIM(PROPERTY_COUNTY)) AS COUNTY
    FROM #ENDORSEMENT_2
    WHERE
        NULLIF(PROPERTY_COUNTY, '') IS NOT NULL
    ;
    
    UPDATE #COUNTY_KEY
    SET
        COUNTY_KEY  = DW_DIMENSIONS.stage.fn_DIM_KEY(
                        'COUNTY'
                        ,@DATA_SOURCE_COUNTY
                        ,NULL
                        ,NULL
                        ,COUNTY
                        ,STATE_KEY
                        ,NULL
                        )
    ;
    
    CREATE NONCLUSTERED INDEX IDX_COUNTY ON #ENDORSEMENT_2 (STATE_KEY, PROPERTY_COUNTY);
    
    
    
    
    
    /*CITY*/
    CREATE TABLE #CITY_KEY (
    STATE_KEY BIGINT
    ,CITY VARCHAR(150)
    ,CITY_KEY BIGINT
    ,CONSTRAINT PK_STATE_CITY PRIMARY KEY (STATE_KEY, CITY)
    );
    
    INSERT INTO #CITY_KEY (
    STATE_KEY
    ,CITY
    )
    SELECT DISTINCT
        STATE_KEY
        ,UPPER(TRIM(PROPERTY_CITY)) AS CITY
    FROM #ENDORSEMENT_2
    WHERE
        NULLIF(PROPERTY_CITY, '') IS NOT NULL
    ;
    
    UPDATE #CITY_KEY
    SET
        CITY_KEY = DW_DIMENSIONS.stage.fn_DIM_KEY(
                    'CITY'
                    ,@DATA_SOURCE_CITY
                    ,NULL
                    ,NULL
                    ,CITY
                    ,STATE_KEY
                    ,NULL
                    )
    ;
    
    CREATE NONCLUSTERED INDEX IDX_CITY ON #ENDORSEMENT_2 (STATE_KEY, PROPERTY_CITY);
    
    
    
    INSERT INTO #ENDORSEMENT_3 (
    [PROPERTY_STATE_CODE]
    ,[STATE_KEY]
    ,[STATE_CODE]
    ,[COUNTY_KEY]
    ,[PROPERTY_COUNTY]
    ,[CITY_KEY]
    ,[PROPERTY_CITY]
    ,[PROPERTY_ZIP_CODE]
    ,[ORIGINATOR_KEY]
    ,[ORIGINATING_MORTGAGEE_SPONSOR_ORIGINATOR]
    ,[ISSUER_KEY]
    ,[ISSUER]
    ,[ORIGINATING_MORTGAGEE_NUMBER]
    ,[SPONSOR_NAME]
    ,[SPONSOR_NUMBER]
    ,[SPONSOR_ORIGINATOR]
    ,[CURRENT_SERVICER_ID]
    ,[PREVIOUS_SERVICER]
    ,[NMLS_ID]
    ,[STANDARD_SAVER]
    ,[PURCHASE_REFINANCE_KEY]
    ,[PURCHASE_REFINANCE]
    ,[HECM_TYPE_KEY]
    ,[HECM_TYPE]
    ,[RATE_TYPE_KEY]
    ,[RATE_TYPE]
    ,[INTEREST_RATE]
    ,[INITIAL_PRINCIPAL_LIMIT]
    ,[MAXIMUM_CLAIM_AMOUNT]
    ,[ENDORSEMENT_YEAR]
    ,[ENDORSEMENT_MONTH]
    ,[DATA_SOURCE]
    )
    SELECT
        E.[PROPERTY_STATE_CODE]
        ,E.[STATE_KEY]
        ,E.[STATE_CODE]
        ,CNTY.COUNTY_KEY	
        ,E.[PROPERTY_COUNTY]
        ,CITY.CITY_KEY
        ,E.[PROPERTY_CITY]
        ,E.[PROPERTY_ZIP_CODE]
        ,E.[ORIGINATOR_KEY]
        ,E.[ORIGINATING_MORTGAGEE_SPONSOR_ORIGINATOR]
        ,E.[ISSUER_KEY]
        ,E.[ISSUER]
        ,E.[ORIGINATING_MORTGAGEE_NUMBER]
        ,E.[SPONSOR_NAME]
        ,E.[SPONSOR_NUMBER]
        ,E.[SPONSOR_ORIGINATOR]
        ,E.[CURRENT_SERVICER_ID]
        ,E.[PREVIOUS_SERVICER]
        ,E.[NMLS_ID]
        ,E.[STANDARD_SAVER]
        ,E.[PURCHASE_REFINANCE_KEY]
        ,E.[PURCHASE_REFINANCE]
        ,E.[HECM_TYPE_KEY]
        ,E.[HECM_TYPE]
        ,E.[RATE_TYPE_KEY]
        ,E.[RATE_TYPE]
        ,E.[INTEREST_RATE]
        ,E.[INITIAL_PRINCIPAL_LIMIT]
        ,E.[MAXIMUM_CLAIM_AMOUNT]
        ,E.[ENDORSEMENT_YEAR]
        ,E.[ENDORSEMENT_MONTH]
        ,E.[DATA_SOURCE]
    FROM #ENDORSEMENT_2 E
    LEFT JOIN #COUNTY_KEY CNTY
        ON E.STATE_KEY = CNTY.STATE_KEY
        AND UPPER(TRIM(E.PROPERTY_COUNTY)) = CNTY.COUNTY
    LEFT JOIN #CITY_KEY CITY
        ON E.STATE_KEY = CITY.STATE_KEY
        AND UPPER(TRIM(E.PROPERTY_CITY)) = CITY.CITY
    ;
    
    
    INSERT INTO stage.HECM_ENDORSEMENT WITH(TABLOCK) (
    [PROPERTY_STATE_CODE]
    ,[STATE_KEY]
    ,[STATE_CODE]
    ,[COUNTY_KEY]
    ,[PROPERTY_COUNTY]
    ,[CITY_KEY]
    ,[PROPERTY_CITY]
    ,[PROPERTY_ZIP_CODE]
    ,[ORIGINATOR_KEY]
    ,[ORIGINATING_MORTGAGEE_SPONSOR_ORIGINATOR]
    ,[ISSUER_KEY]
    ,[ISSUER]
    ,[ORIGINATING_MORTGAGEE_NUMBER]
    ,[SPONSOR_NAME]
    ,[SPONSOR_NUMBER]
    ,[SPONSOR_ORIGINATOR]
    ,[CURRENT_SERVICER_ID]
    ,[PREVIOUS_SERVICER]
    ,[NMLS_ID]
    ,[STANDARD_SAVER]
    ,[PURCHASE_REFINANCE_KEY]
    ,[PURCHASE_REFINANCE]
    ,[HECM_TYPE_KEY]
    ,[HECM_TYPE]
    ,[RATE_TYPE_KEY]
    ,[RATE_TYPE]
    ,[INTEREST_RATE]
    ,[INITIAL_PRINCIPAL_LIMIT]
    ,[MAXIMUM_CLAIM_AMOUNT]
    ,[ENDORSEMENT_YEAR]
    ,[ENDORSEMENT_MONTH]
    ,[DATA_SOURCE]
    ,[CREATED_DATE]
    ,[CREATED_USER]
    )
    SELECT
        [PROPERTY_STATE_CODE]
        ,[STATE_KEY]
        ,[STATE_CODE]
        ,[COUNTY_KEY]
        ,[PROPERTY_COUNTY]
        ,[CITY_KEY]
        ,[PROPERTY_CITY]
        ,[PROPERTY_ZIP_CODE]
        ,[ORIGINATOR_KEY]
        ,[ORIGINATING_MORTGAGEE_SPONSOR_ORIGINATOR]
        ,[ISSUER_KEY]
        ,[ISSUER]
        ,[ORIGINATING_MORTGAGEE_NUMBER]
        ,[SPONSOR_NAME]
        ,[SPONSOR_NUMBER]
        ,[SPONSOR_ORIGINATOR]
        ,[CURRENT_SERVICER_ID]
        ,[PREVIOUS_SERVICER]
        ,[NMLS_ID]
        ,[STANDARD_SAVER]
        ,[PURCHASE_REFINANCE_KEY]
        ,[PURCHASE_REFINANCE]
        ,[HECM_TYPE_KEY]
        ,[HECM_TYPE]
        ,[RATE_TYPE_KEY]
        ,[RATE_TYPE]
        ,[INTEREST_RATE]
        ,[INITIAL_PRINCIPAL_LIMIT]
        ,[MAXIMUM_CLAIM_AMOUNT]
        ,[ENDORSEMENT_YEAR]
        ,[ENDORSEMENT_MONTH]
        ,[DATA_SOURCE]
        ,GETDATE() AS CREATED_DATE
        ,DATABASE_PRINCIPAL_ID() AS CREATED_USER
    FROM #ENDORSEMENT_3
    ;

    """

    cursor.execute(insert_sql)


print('success')
