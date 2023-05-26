from itertools import islice
import pandas as pd
import sys
import sqlalchemy

in_file = r'C:\Users\jasonwalker\OneDrive - SitusAMC\Documents\project\baseline\source_file\202112\vectors_January_Small.csv'
out_file = r'c:\temp\vectors_202112.csv'
# dsn = r'DW_DEV_DW_LOAN_VALUATION'
# target_table = r'VECTORS_202112'

data = []


with open(in_file) as f:
    for line in islice(f, 10000):
        partial_prepay = str(line).split(sep=',')[23]
        if 'e' in partial_prepay:
            print(line)
            sys.exit()

# with open(in_file) as f:
#     for line in f:
#         data_iter = [
#             str(line).split(sep=',')[0]
#             , str(line).split(sep=',')[1]
#             , str(line).split(sep=',')[6]
#             , str(line).split(sep=',')[7]
#             , str(line).split(sep=',')[17]
#             , str(line).split(sep=',')[9]
#             , str(line).split(sep=',')[12]
#             , str(line).split(sep=',')[21]
#             , str(line).split(sep=',')[23]
#             , str(line).split(sep=',')[22]
#             ]
#         data.append(data_iter)


column = [
    'DISCLOSURE_SEQUENCE_NUMBER'
    , 'SEQUENCE_NUMBER_SUFFIX'
    , 'HECM_LOAN_CURRENT_INTEREST_RATE'
    , 'CURRENT_HECM_UPB'
    , 'PARTICIPATION_UPB_FOLLOWING_MONTH'
    , 'CURRENT_HECM_NOTE_RATE'
    , 'CURRENT_LINE_OF_CREDIT'
    , 'SCHEDULED_BALANCE'
    , 'PARTIAL_PREPAY'
    , 'FULL_PREPAY'
]

df = pd.DataFrame(data, columns=column)

# df['HECM_LOAN_CURRENT_INTEREST_RATE'] = df['HECM_LOAN_CURRENT_INTEREST_RATE'].astype('float')
# df['CURRENT_HECM_UPB'] = df['CURRENT_HECM_UPB'].astype('float')
# df['PARTICIPATION_UPB_FOLLOWING_MONTH'] = df['PARTICIPATION_UPB_FOLLOWING_MONTH'].astype('float')
# df['CURRENT_HECM_NOTE_RATE'] = df['CURRENT_HECM_NOTE_RATE'].astype('float')
# df['CURRENT_LINE_OF_CREDIT'] = df['CURRENT_LINE_OF_CREDIT'].astype('float')
# df['SCHEDULED_BALANCE'] = df['SCHEDULED_BALANCE'].astype('float')
# df['PARTIAL_PREPAY'] = df['PARTIAL_PREPAY'].astype('float')
# df['FULL_PREPAY'] = df['FULL_PREPAY'].astype('float')

# engine = sqlalchemy.create_engine(
#         f"mssql+pyodbc://@{dsn}",
#         fast_executemany=True)
#
# conn = engine.connect()
#
# df.to_sql(target_table, conn, if_exists='append', index=False)

df.to_csv(out_file, index=False)

print('success')
