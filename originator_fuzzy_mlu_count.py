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

env = env_info['uat']
db = r'DW_LOAN_VALUATION'
source_file = r'c:\temp\GNMA_ORIGINATOR.txt'


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

def first_phrase(text):
    delimiter_count = [i for i, s in enumerate(text) if s == ' ']
    if len(delimiter_count) > 0:
        # return delimiter_count
        return text[:delimiter_count[0]]
    else:
        return text



# main program

conn01 = pyodbc.connect(
    f"Driver={driver};"
    f"Server={server};"
    f"Database={db};"
    f"UID={uname};"
    f"PWD={pword};"
    , autocommit=True)


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


df_source = pd.read_csv(filepath_or_buffer=source_file, names=['SOURCE'])


# df_source.to_csv(path_or_buf=r'c:\temp\out.csv', index=False)
# os.startfile(r'c:\temp\out.csv')
# sys.exit()


df_source['SOURCE_CLEAN'] = df_source['SOURCE'].apply(lambda x: replace_text(x))
df_source['FIRST_PHRASE'] = df_source['SOURCE_CLEAN'].apply(lambda x: first_phrase(x))

df_mlu = pd.read_sql(sql=sql_mlu_client, con=conn01)
df_mlu['MLU_CLIENT_NAME_CLEAN'] = df_mlu['MLU_CLIENT_NAME'].apply(lambda x: replace_text(x))
df_mlu['FIRST_PHRASE'] = df_mlu['MLU_CLIENT_NAME_CLEAN'].apply(lambda x: first_phrase(x))

df_combo = df_source.merge(right=df_mlu, how='left', on='FIRST_PHRASE')

df_combo.to_csv(path_or_buf=r'c:\temp\out.csv', index=False)
os.startfile(r'c:\temp\out.csv')



print('success')
