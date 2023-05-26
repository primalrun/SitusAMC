import pandas as pd
import os
import sys
import math

out_file = r'c:\temp\test1.xlsx'
out_file_sql = r'c:\temp\table_create.txt'
in_file = r'c:\temp\vectors_count.txt'
out_file_dir = r'c:\temp'
vector_insert_sql = r'INSERT INTO DW_LOAN_VALUATION.[stage].[SRC_Vectors] '
vector_select_prefix = r'SELECT * FROM DW_LOAN_VALUATION.stage.'

df = pd.read_csv(in_file, sep='\t')
df['CUMULATIVE_SUM'] = df['ROW_COUNT'].cumsum()
bins = [x for x in range(0, 516000000, 1000000)]
labels = ['SRC_Vectors_' + str(i + 1) for i, x in enumerate(bins[1:])]
df['CATEGORY'] = pd.cut(df.CUMULATIVE_SUM, bins=bins, labels=labels)
tables = df['CATEGORY'].unique().tolist()
sql_out = ''

for x in tables:
    table = x

    sql_create_table = f"""
    CREATE TABLE [stage].[{table}](
        [DisclosureSequenceNumber] [bigint] NOT NULL,
        [SequenceNumberSuffix] [varchar](3) NOT NULL,
        [PoolID] [varchar](100) NOT NULL,
        [HECMOriginalFundingDate] [date] NULL,
        [AsOfDate] [date] NOT NULL,
        [MCA] [varchar](10) NULL,
        [HECMLoanCurrentInterestRate] [decimal](24, 8) NULL,
        [CurrentHECMUPB] [decimal](24, 8) NULL,
        [CurrentHECMMIPRate] [decimal](24, 8) NULL,
        [CurrentHECMNoteRate] [decimal](24, 8) NULL,
        [ParticipationUPBcurrent] [decimal](24, 8) NULL,
        [FutureHECMUPB] [decimal](24, 8) NULL,
        [CurrentLineOfCredit] [decimal](24, 8) NULL,
        [FollowingMonthLineofCredit] [decimal](24, 8) NULL,
        [MIPBasisPoints] [decimal](24, 8) NULL,
        [MonthlyServicingFeeAmount] [decimal](24, 8) NULL,
        [Seasoning] [int] NULL,
        [ParticipationUPBFollowingmonth] [decimal](24, 8) NULL,
        [ParticipationInterestRate] [decimal](24, 8) NULL,
        [PaymentReasonCode] [varchar](2) NULL,
        [PropertyChargeSetAsideAmount] [decimal](24, 8) NULL,
        [ScheduledBalance] [decimal](24, 8) NULL,
        [FullPrepay] [decimal](24, 8) NULL,
        [PartialPrepay] [float] NULL,
        [TotalPrepay] [float] NULL,
        [Creditlinesetasideamount] [decimal](24, 8) NULL,
        [InitialMonthlyScheduledPayment] [decimal](24, 8) NULL,
        [InitialRemainingAvailableLineofCredit] [decimal](24, 8) NULL,
        [Assignments] [decimal](24, 8) NULL,
        [CreatedDate] [datetime] NULL,
        [CreatedBy] [varchar](20) NULL
     );
    """

    sql_out += '\n\n' + sql_create_table

with open(out_file_sql, 'w') as w:
    w.write(sql_out)

dict_select = df.groupby('CATEGORY')['DisclosureSequenceNumber'].apply(lambda x: x.values.tolist()).to_dict()
table_counter = [int(str(s).split(sep='_')[2]) for s in dict_select.keys()]

counter_1 = 1
count_reset = int(max(table_counter) / 10)
counter_reset = 0
file_crosswalk = {}
for x in dict_select:
    file_crosswalk[x] = counter_1
    counter_reset += 1
    if counter_reset >= count_reset:
        counter_reset = 0
        counter_1 += 1

insert_file = {}
union_file = {}
for k in dict_select:
    insert_file[k] = os.path.join(out_file_dir, 'insert_' + str(file_crosswalk[k]) + '.txt')
    union_file[k] = os.path.join(out_file_dir, 'union_' + str(file_crosswalk[k]) + '.txt')

# file cleanup
for x in insert_file:
    try:
        os.remove(insert_file[x])
        os.remove(union_file[x])
    except OSError:
        pass

for x in dict_select:
    table = x
    disclosure_string = [str(x) for x in dict_select[x]]
    in_filter = ', '.join(disclosure_string)
    insert_out_file = insert_file[x]

    insert_sql = f"""
    INSERT INTO  DW_LOAN_VALUATION.[stage].[{table}] WITH(TABLOCK)
    SELECT *
    FROM DW_LOAN_VALUATION.stage.vw_SRC_Vectors WITH(NOLOCK)
    WHERE DisclosureSequenceNumber IN ({in_filter});
    """

    with open(insert_out_file, 'a') as a:
        a.write('\n\n' + insert_sql)

select = [[vector_select_prefix + str(k), str(insert_file[k]).replace('insert', 'union')] for k in insert_file]
select_union = []

for i, x in enumerate(select):
    if i == 0:
        select_union.append([x[0], x[1], 0])
    else:
        if select[i][1] == select[i - 1][1]:
            select_union.append([x[0], x[1], 1])
        else:
            select_union.append([x[0], x[1], 0])

select_union = [[' UNION ALL ' + x[0], x[1]] if x[2] == 1 else [x[0], x[1]] for x in select_union]

union_file_unique = set(union_file.values())

# write insert prefix in each union file
for x in union_file_unique:
    with open(x, 'w') as w:
        w.write(vector_insert_sql + '\n')

# write select union
for x in select_union:
    union_out = x[1]
    union_select = x[0]
    with open(union_out, 'a') as w:
        w.write(union_select + '\n')



# df.to_excel(out_file, index=False)
# os.startfile(out_file)
