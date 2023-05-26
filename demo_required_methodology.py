import pandas as pd
import pyodbc
import sys

stg_server = r'[AMC-DLKDWP01.amcfirst.com]'
stg_db = r'[DW_PERFORMANCE_ATTRIBUTION]'

conn01 = pyodbc.connect(
    "Driver={SQL Server};"
    f"Server={stg_server[1:-1]};"
    f"Database={stg_db[1:-1]};"
    "UID=python;"
    "PWD=FZ9H2Pcg=z%-cf?$;"
    , autocommit=True)

sql_select = f"""
SELECT C2.REQUIRED_METHODOLOGY
	,ROUND(C2.ROW_COUNT / CAST(C2.TOTAL_COUNT AS FLOAT), 2) AS PERCENT_TOTAL
FROM (
SELECT C1.REQUIRED_METHODOLOGY
	,C1.ROW_COUNT
	,SUM(C1.ROW_COUNT) OVER() AS TOTAL_COUNT
FROM (
SELECT C.REQUIRED_METHODOLOGY
	,COUNT(*) AS ROW_COUNT
FROM (
SELECT M.REQUIRED_METHODOLOGY
FROM DW_PROPERTY_VALUATION.dbo.FACT_PROPERTY_VALUATION_NEW F WITH(NOLOCK)
LEFT JOIN DW_PROPERTY_VALUATION.dbo.vw_FACT_REQUIRED_METHODOLOGY M WITH(NOLOCK) ON F.PROPERTY_KEY = M.PROPERTY_KEY
	AND F.VALUATION_DATE_KEY = M.VALUATION_DATE
WHERE M.REQUIRED_METHODOLOGY IS NOT NULL
) C
GROUP BY  C.REQUIRED_METHODOLOGY
) C1
) C2
WHERE ROUND(C2.ROW_COUNT / CAST(C2.TOTAL_COUNT AS FLOAT), 2) > .05;
"""


def percent_total_adjustment(percent_total):
    if percent_total < 1:
        return round(1 - percent_total, 2)
    elif percent_total > 1:
        return round(percent_total - 1, 2)
    else:
        return 0

def history_assignment_balance(assignment_total, property_total):
    if assignment_total < property_total:
        return property_total - assignment_total
    elif assignment_total > property_total:
        return assignment_total - property_total
    else:
        return 0

data = pd.read_sql(sql=sql_select, con=conn01)
total_percent = data['PERCENT_TOTAL'].sum()

# add percent_total adjustment to last index of dataframe
data.at[data.index[-1], 'PERCENT_TOTAL'] += percent_total_adjustment(total_percent)

# add row for every comma delimiter in REQUIRED_METHODOLOGY
data_list = data.values.tolist()
expand_list = []
history_row = 1

for x in data_list:
    required_methodology = x[0]
    percent_total = x[1]
    required_methodology_split = required_methodology.split(', ')
    split_length = len(required_methodology_split)

    if not required_methodology_split:
        expand_list.append([required_methodology, percent_total, history_row])
    else:
        for y in range(0, split_length):
            required_methodology_iter = required_methodology_split[y]
            expand_list.append([required_methodology_iter, percent_total, history_row])

    history_row += 1

df_required_methodology = pd.DataFrame(expand_list, columns=['REQUIRED_METHODOLOGY', 'PERCENT_TOTAL', 'HISTORY_ROW'])


# required_methodology_key
required_methodology_key_sql = """
SELECT REQUIRED_METHODOLOGY_KEY
	,REQUIRED_METHODOLOGY
FROM DW_PROPERTY_VALUATION.[dbo].[vw_DIM_REQUIRED_METHODOLOGY]
"""
df_rm_key = pd.read_sql(sql=required_methodology_key_sql, con=conn01)
df_required_methodology = pd.merge(df_required_methodology, df_rm_key, how='inner', on='REQUIRED_METHODOLOGY')
df_required_methodology['REQUIRED_METHODOLOGY_KEY'].astype('int')
df_history_rmk = df_required_methodology[['HISTORY_ROW', 'REQUIRED_METHODOLOGY_KEY']]

# unique client and property to assign required methodology
sql_property = f"""
SELECT DISTINCT DIF.CLIENT_KEY
	,DIF.PROPERTY_KEY
FROM DW_PROPERTY_VALUATION.stage.SRC_DEMO_INTERNATIONAL_FACTS DIF WITH(NOLOCK);
"""

df_property = pd.read_sql(sql=sql_property, con=conn01)
percent_history = df_required_methodology[['PERCENT_TOTAL', 'HISTORY_ROW']].drop_duplicates().values.tolist()
property_count = df_property['PROPERTY_KEY'].count()

history_assignment = []

for x in percent_history:
    history = x[1]
    assign_ratio = x[0]
    assignment_rows = round((property_count * assign_ratio), 0)
    history_assignment.append([history, assign_ratio, assignment_rows])


assignment_row_total = sum([x[2] for x in history_assignment])
history_assignment[-1][2] += history_assignment_balance(assignment_row_total, property_count)
history_insert = []

for x in history_assignment:
    for y in range(0, int(x[2])):
        history_insert.append(int(x[0]))

df_property['HISTORY_ROW'] = history_insert


df_property_method = pd.merge(
    df_property[['CLIENT_KEY', 'PROPERTY_KEY', 'HISTORY_ROW']],
    df_required_methodology[['REQUIRED_METHODOLOGY_KEY', 'HISTORY_ROW']],
    how='inner', on='HISTORY_ROW')

print(df_property_method)
# pandas library on SQL Server does not support explode
# data_explode = data.assign(REQUIRED_METHODOLOGY=data['REQUIRED_METHODOLOGY'].str.split(', ')).explode(
#     'REQUIRED_METHODOLOGY')
