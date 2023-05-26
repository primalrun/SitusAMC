import pandas as pd
import pyodbc
import sqlalchemy
import sys

server = r'AMC-DLKDWP01.amcfirst.com'
db = r'DW_LOAN_VALUATION'
dsn = r'DW_DEV_DW_LOAN_VALUATION'


conn_dw_prod = pyodbc.connect(
    "Driver={SQL Server};"
    f"Server={server};"
    f"Database={db};"
    "UID=python;"
    "PWD=FZ9H2Pcg=z%-cf?$;"
    , autocommit=True)

sql_select = """
SELECT RANDOM_CATEGORY
	,POOL_ID
FROM (
SELECT POOL_ID
	,ABS(CHECKSUM(NEWID())) % 25 + 1 AS RANDOM_CATEGORY
FROM DW_LOAN_VALUATION.stage.HMBS_PORTFOLIO_LOAN_LEVEL WITH(NOLOCK)
GROUP BY  POOL_ID
) A
ORDER BY RANDOM_CATEGORY
"""

df_in = pd.read_sql(sql=sql_select, con=conn_dw_prod)
category_pool_id = df_in.values.tolist()
category = set(df_in['RANDOM_CATEGORY'].unique())

# print(category_pool_id[0])
# sys.exit()

for x in category:
    category_iter = x
    pool_id_str = ', '.join(["'" + x[1] + "'" for x in category_pool_id if x[0] == category_iter])
    target_table_iter = f'HMBS_PORTFOLIO_LOAN_LEVEL_{category_iter}'

    sql_iter = f"""
    SELECT *
    FROM DW_LOAN_VALUATION.stage.HMBS_PORTFOLIO_LOAN_LEVEL WITH(NOLOCK)
    WHERE POOL_ID IN ({pool_id_str})
    """

    df_copy = pd.read_sql(sql=sql_iter, con=conn_dw_prod)

    engine = sqlalchemy.create_engine(
        f"mssql+pyodbc://@{dsn}",
        fast_executemany=True)

    con_dw_dev = engine.connect()

    df_copy.to_sql(target_table_iter, con_dw_dev, if_exists='append', index=False)
    print(str(category_iter) + ' inserted')

