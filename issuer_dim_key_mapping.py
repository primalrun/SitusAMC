import pandas as pd
import pyodbc
import re
import sqlalchemy
import os
import sys

# variable


# environment: (server, driver, uname, pword, dsn)
env_info = {'dev': (r'COM-DLKDWD01.SITUSAMC.COM', r'ODBC Driver 17 for SQL Server', r'python', r'FZ9H2Pcg=z%-cf?$',
                    r'DW_DEV_DW_LOAN_VALUATION')
    , 'uat': (r'COM-DWDBU01.SITUSAMC.COM', r'ODBC Driver 17 for SQL Server', r'python', r'FZ9H2Pcg=z%-cf?$',
              r'DW_UAT_DW_LOAN_VALUATION')
    , 'prod': (r'COM-DWAGP01.SITUSAMC.COM', r'SQL Server', r'python', r'FZ9H2Pcg=z%-cf?$', r'DW_PROD_DW_LOAN_VALUATION')
            }

env = env_info['dev']
db = r'DW_LOAN_VALUATION'

server = env[0]
driver = env[1]
uname = env[2]
pword = env[3]
dsn = env[4]

replace_dict = {
    ' MTGE': ' MTG'
    , ' MORTGAGE': ' MTG'
    , ' MORTAGE': ' MTG'
    , ' MORTGAG': ' MTG'
    , ' MORTG': ' MTG'
    , ' INCORPORATED': ' INC'
    , ',': ' '
    , '.': ' '
    , '-': ' '
    , "'": ' '
    , ' CORPORATION': ' CORP'
    , ' CORPORAT': ' CORP'
    , ' FINANCIAL': ' FNCL'
    , ' FINANCL': ' FNCL'
    , ' FINL': ' FNCL'
    , ' GROUP': ' GRP'
    , ' ENTERPRISE': ' ENTPRS'
    , ' ENTERPRISES': ' ENTPRS'
    , ' AND ': ' & '
    , ' FEDERAL SAVINGS BANK': ' FSB'
    , ' COMPANY': ' CO'
    , ' INTERNATL': ' INTL'
    , ' INTERNATIONAL': ' INTL'
    , ' TR ': ' TRUST '
    , ' SERV ': ' SVCS '
    , ' SERVS ': ' SVCS '
    , ' SRVS ': ' SVCS  '
    , ' SERVICES': ' SVCS'
    , ' SERVICE': ' SVCS'
    , ' SERVIC': ' SVCS'
    , ' SERVI': ' SVCS'
    , ' SOLUTIONS': ' SLTNS'
    , ' RESIDENTIAL': ' RSDNTL'
    , ' CR ': ' CREDIT '
    , ' UN ': ' UNION '
    , ' S ': ' SAVINGS '
    , ' LN ': ' LOAN '
    , ' ASSN': ' ASSOCN'
    , ' ASSOC1': ' ASSOCT'
    , ' ASSOC': ' ASSOCT'
    , ' ASSCNIATES': ' ASSOCT'
    , ' ASSOCIATI': ' ASSOCT'
    , 'N.A': 'NA'
    , ' HM ': ' HOME '
    , ' INVESTMENT': ' INV'
    , ' INVESTME': ' INV'
    , ' INVESTMEN': ' INV'
    , ' DEVELOP': ' DVLPMNT'
    , ' HOUSING': ' HSNG'
    , ' HSG': ' HSNG'
    , ' AGEN': ' AGCY'
    , ' AGENCY': ' AGCY'
    , ' PROFESSIONAL': ' PROFSNL'
    , ' PROFESSIONALS': ' PROFSNL'
    , ' PROFESSIO': ' PROFSNL'
    , ' PROFESS': ' PROFSNL'
    , ' MANAGEMENT': ' MGMT'
    , ' MANAGEM': ' MGMT'
    , r' D/B/A': 'DBA'
    , 'V.I.P.': 'VIP'
}


# function
def replace_text(text):
    if len(text) == 0:
        return None

    str_upper = str(text).strip().upper()
    replacement = dict((re.escape(k), v) for k, v in replace_dict.items())
    pattern = re.compile('|'.join(replacement.keys()))
    str_replace = pattern.sub(lambda m: replacement[re.escape(m.group(0))], str_upper)
    if len(str_replace) > 0:
        return str_replace.replace('  ', ' ')
    else:
        return None

def fuzzy_search(text, occurrence):
    # get substring of client before nth (occurrence) space
    delimiter_count = [i for i, s in enumerate(text) if s == ' ']
    if len(delimiter_count) >= occurrence:
        return text[:delimiter_count[occurrence - 1]]
    else:
        return None


# main program

conn01 = pyodbc.connect(
    f"Driver={driver};"
    f"Server={server};"
    f"Database={db};"
    f"UID={uname};"
    f"PWD={pword};"
    , autocommit=True)

sql_source = """
SELECT
	B.ORIGINATOR AS SOURCE
FROM (
SELECT
	A.ORIGINATOR
	,DW_DIMENSIONS.stage.fn_CLIENT_KEY(
		'ORIGINATOR'
		,130044
		,NULL
		,NULL
		,A.ORIGINATOR
		) AS CLIENT_KEY
FROM (
SELECT DISTINCT
	UPPER(TRIM(SPONSOR_ORIGINATOR)) AS ORIGINATOR
FROM stage.HECM_ENDORSEMENT
WHERE
	NULLIF(SPONSOR_ORIGINATOR, '') IS NOT NULL	
) A
) B
WHERE
	B.CLIENT_KEY = 100000
"""

sql_mlu_client = """

SELECT
	CLIENT_KEY AS MLU_CLIENT_KEY
	,CLIENT_NAME AS MLU_CLIENT_NAME	
FROM DW_DIMENSIONS.dbo.MLU_CLIENT WITH(NOLOCK) 
WHERE
	CLIENT_NAME <> 'UNKNOWN'
ORDER BY
    MLU_CLIENT_NAME
"""

sql_ref_client = """
SELECT DISTINCT
	UPPER(TRIM(CLIENT_ALIAS)) AS CLIENT_ALIAS
	,CLIENT_KEY AS REF_CLIENT_KEY
FROM DW_DIMENSIONS.dbo.REF_CLIENT WITH(NOLOCK)
ORDER BY
	UPPER(TRIM(CLIENT_ALIAS))
"""

df_source = pd.read_sql(sql=sql_source, con=conn01)
df_source['SOURCE_CLEAN'] = df_source['SOURCE'].apply(lambda x: replace_text(x))

df_mlu = pd.read_sql(sql=sql_mlu_client, con=conn01)
df_mlu['MLU_CLIENT_NAME_CLEAN'] = df_mlu['MLU_CLIENT_NAME'].apply(lambda x: replace_text(x))

df_mlu_max = df_mlu.groupby('MLU_CLIENT_NAME_CLEAN').agg({'MLU_CLIENT_NAME': 'max'}).copy()
df_mlu_max.reset_index(inplace=True)

df_mlu_dedupe = df_mlu_max.merge(
    right=df_mlu[['MLU_CLIENT_KEY', 'MLU_CLIENT_NAME']], how='left', on='MLU_CLIENT_NAME').copy()

df_mlu_dedupe['SOURCE_CLEAN'] = df_mlu_dedupe['MLU_CLIENT_NAME_CLEAN']

df_ref_client = pd.read_sql(sql=sql_ref_client, con=conn01)
df_ref_client['CLIENT_ALIAS_CLEAN'] = df_ref_client['CLIENT_ALIAS'].apply(lambda x: replace_text(x))
df_ref_client['SOURCE_CLEAN'] = df_ref_client['CLIENT_ALIAS_CLEAN']



df_source_mlu = df_source.merge(
    right=df_mlu_dedupe[['SOURCE_CLEAN', 'MLU_CLIENT_NAME']], how='left', on='SOURCE_CLEAN').copy()

df_ref_client_lookup = df_source_mlu[['SOURCE_CLEAN']].merge(
    right=df_ref_client[['SOURCE_CLEAN', 'REF_CLIENT_KEY']], how='inner', on='SOURCE_CLEAN').copy()

df_ref_client_dedupe = df_ref_client_lookup.groupby('SOURCE_CLEAN').agg({'REF_CLIENT_KEY': 'max'}).copy()
df_ref_client_dedupe.reset_index(inplace=True)
df_ref_client_dedupe['MLU_CLIENT_KEY'] = df_ref_client_dedupe['REF_CLIENT_KEY']

df_ref_client = df_ref_client_dedupe.merge(
    right=df_mlu_dedupe[['MLU_CLIENT_KEY', 'MLU_CLIENT_NAME']], how='left', on='MLU_CLIENT_KEY'
)

df_ref_client['REF_CLIENT_NAME'] = df_ref_client['MLU_CLIENT_NAME']

df_mapping = df_source_mlu.merge(
    right=df_ref_client[['SOURCE_CLEAN', 'REF_CLIENT_NAME']], how='left', on='SOURCE_CLEAN'
)

df_mapping['SOURCE_CLEAN_FUZZY_2'] = df_mapping['SOURCE_CLEAN'].map(lambda x: fuzzy_search(x, 2))

df_mlu_fuzzy = df_mlu_dedupe.copy()
df_mlu_fuzzy['SOURCE_CLEAN_FUZZY_2'] = df_mlu_fuzzy['SOURCE_CLEAN'].map(lambda x: fuzzy_search(x, 2))
df_mlu_fuzzy = df_mlu_fuzzy.groupby('SOURCE_CLEAN_FUZZY_2').agg({'MLU_CLIENT_NAME': 'max'}).copy()
df_mlu_fuzzy.reset_index(inplace=True)
df_mlu_fuzzy['MLU_CLIENT_NAME_FUZZY_2'] = df_mlu_fuzzy['MLU_CLIENT_NAME']

df_mapping = pd.merge(
    df_mapping
    , right=df_mlu_fuzzy[['SOURCE_CLEAN_FUZZY_2', 'MLU_CLIENT_NAME_FUZZY_2']]
    , how='left'
    , on='SOURCE_CLEAN_FUZZY_2'
)

df_mapping['MLU_CLIENT_NAME_MAPPING'] = None

df_mapping.to_csv(path_or_buf=r'c:\temp\out.csv', index=False)
os.startfile(r'c:\temp\out.csv')



print('success')
