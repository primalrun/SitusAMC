import pyodbc
import pandas as pd


def environment_variables(environment):
    # environment: (server, driver, uname, pword, dsn)
    env_info = {'dev': (r'COM-DLKDWD01.SITUSAMC.COM', r'ODBC Driver 17 for SQL Server', r'python', r'FZ9H2Pcg=z%-cf?$',
                        r'DW_DEV_DW_LOAN_VALUATION')
        , 'uat': (r'COM-DWDBU01.SITUSAMC.COM', r'ODBC Driver 17 for SQL Server', r'python', r'FZ9H2Pcg=z%-cf?$',
                  r'DW_UAT_DW_LOAN_VALUATION')
        , 'prod': (
            r'COM-DWAGP01.SITUSAMC.COM', r'SQL Server', r'python', r'FZ9H2Pcg=z%-cf?$', r'DW_PROD_DW_LOAN_VALUATION')
        , 'data_lake': (
            r'COM-DLKAGP01.SITUSAMC.COM', r'SQL Server', None, None, None)
                }
    env = env_info[environment]
    server = env[0]
    driver = env[1]
    uname = env[2]
    pword = env[3]
    dsn = env[4]

    return server, driver, uname, pword, dsn


class ConnectSQLServer:
    def __init__(self, server, driver, uname, pword):
        self.server = server
        self.driver = driver
        self.uname = uname
        self.pword = pword

    def connection(self, authentication, db):
        if authentication == 'sql server':
            conn = pyodbc.connect(
                f"Driver={self.driver};"
                f"Server={self.server};"
                f"Database={db};"
                f"UID={self.uname};"
                f"PWD={self.pword};"
                , autocommit=True)
        if authentication == 'windows':
            conn = pyodbc.connect(
                f"Driver={self.driver};"
                f"Server={self.server};"
                f"Database={db};"
                f"Trusted_Connection=yes;"
                , autocommit=True)
        return conn


def schema_objects(schema, object_type):
    if object_type == 'table':
        sql = f"""
        SELECT
            T.NAME AS OBJECT_NAME
        FROM SYS.TABLES T WITH(NOLOCK)
        INNER JOIN SYS.SCHEMAS S WITH(NOLOCK)
            ON T.SCHEMA_ID = S.SCHEMA_ID
        WHERE	
            S.NAME = '{schema}'        
        """

    if object_type == 'view':
        sql = f"""
        SELECT
            V.NAME AS OBJECT_NAME
        FROM SYS.VIEWS V WITH(NOLOCK)
        INNER JOIN SYS.SCHEMAS S WITH(NOLOCK)
            ON V.SCHEMA_ID = S.SCHEMA_ID
        WHERE	
            S.NAME = '{schema}'
        """

    return sql


def table_column_attributes(schema, table):
    sql = f"""
    SELECT
        T.NAME AS TABLE_NAME
        ,S.NAME AS SCHEMA_NAME
        ,C.COLUMN_ID
        ,C.NAME AS COLUMN_NAME
        ,TP.NAME AS DATA_TYPE
        ,C.MAX_LENGTH
        ,C.PRECISION
        ,C.SCALE
        ,C.IS_NULLABLE
        ,C.IS_IDENTITY
    FROM SYS.TABLES T WITH(NOLOCK)
    INNER JOIN SYS.SCHEMAS S WITH(NOLOCK)
        ON T.SCHEMA_ID = S.SCHEMA_ID
    INNER JOIN SYS.COLUMNS C WITH(NOLOCK)
        ON T.OBJECT_ID = C.OBJECT_ID
    INNER JOIN SYS.TYPES TP WITH(NOLOCK)
        ON C.USER_TYPE_ID = TP.USER_TYPE_ID
    WHERE
        T.NAME = '{table}'
        AND S.NAME = '{schema}'
    ORDER BY
        C.COLUMN_ID    
    """

    return sql


def quote_string(string, quote_char_begin):
    quote_char_dict = {'[': ']'
        , '(': ')'
        , '{': '}'}

    return f"{quote_char_begin}{string}{quote_char_dict[quote_char_begin]}"


def column_data_type(column, data_type, max_length, precision, scale, is_null):
    column_iter = quote_string(column, '[')
    data_type_iter = quote_string(str(data_type).upper(), '[')

    if is_null == 0:
        null_iter = 'NOT NULL'
    else:
        null_iter = 'NULL'

    if str(data_type).upper() in {'VARCHAR', 'CHAR', 'NVARCHAR'}:
        data_size_iter = quote_string(str(max_length), '(')
        return f'{column_iter} {data_type_iter}{data_size_iter} {null_iter}'

    if str(data_type).upper() in {'INT', 'BIGINT', 'SMALLINT', 'TINYINT', 'DATETIME', 'DATE', 'BIT'}:
        return f'{column_iter} {data_type_iter} {null_iter}'

    if str(data_type).upper() in {'DECIMAL'}:
        data_size_iter = quote_string(str(precision) + ', ' + str(scale), '(')
        return f'{column_iter} {data_type_iter}{data_size_iter} {null_iter}'

    else:
        return f'{column_iter} {data_type_iter} {null_iter}'


def source_view_script(source_server
                       , source_db
                       , source_schema
                       , source_object
                       , target_db
                       , target_schema
                       , column_name):
    use_db = f'USE {target_db}' + '\n' + 'GO' + ('\n' * 2)
    drop_view = f'DROP VIEW IF EXISTS {target_schema}.vw_SRC_{source_object}' + '\n' + 'GO' + ('\n' * 2)
    create_view = f'CREATE VIEW {target_schema}.vw_SRC_{source_object}' + '\n' + 'AS' + ('\n' * 2)
    select = 'SELECT' + '\n'
    select_col = '\n,'.join(column_name) + '\n'
    from_server = quote_string(source_server, '[')
    from_db = quote_string(source_db, '[')
    from_schema = quote_string(source_schema, '[')
    from_object = quote_string(source_object, '[')
    from_obj = f'FROM {from_server}.{from_db}.{from_schema}.{from_object} WITH(NOLOCK)'

    return f'{use_db}{drop_view}{create_view}{select}{select_col}{from_obj}'


def source_stage_table_script(source_object
                              , target_db
                              , target_schema
                              , column_ddl):
    use_db = f'USE {target_db}' + '\n' + 'GO' + ('\n' * 2)
    drop_table = f'DROP TABLE IF EXISTS {target_schema}.SRC_{source_object}' + '\n' + 'GO' + ('\n' * 2)
    create_table = f'CREATE TABLE {target_schema}.SRC_{source_object} (' + '\n'
    column_ddl = '\n,'.join(column_ddl) + '\n' + ')' + '\n'
    return f'{use_db}{drop_table}{create_table}{column_ddl}'


def table_column_null_check_sql(p_schema, p_object, p_con):
    sql = f"""
    SELECT
        C.NAME AS COLUMN_NAME
    FROM SYS.TABLES T WITH(NOLOCK)
    INNER JOIN SYS.SCHEMAS S WITH(NOLOCK)
        ON T.SCHEMA_ID = S.SCHEMA_ID
    INNER JOIN SYS.COLUMNS C WITH(NOLOCK)
        ON T.OBJECT_ID = C.OBJECT_ID
    INNER JOIN SYS.TYPES TP WITH(NOLOCK)
        ON C.USER_TYPE_ID = TP.USER_TYPE_ID
    WHERE
        T.NAME = '{p_object}'
        AND S.NAME = '{p_schema}'
    ORDER BY
        C.COLUMN_ID
    """

    column_name_list = pd.read_sql(sql=sql, con=p_con).values.tolist()
    column_name = [c[0] for c in column_name_list]
    column_select = []
    for c in column_name:
        str_iter = f'SUM(CASE WHEN {c} IS NOT NULL THEN 1 ELSE 0 END) AS {c}_NOT_NULL_COUNT'
        column_select.append(str_iter)
    a_select = 'SELECT' + '\n'
    a_column = ',\n'.join(column_select) + '\n'
    a_from = f'FROM {p_schema}.{p_object} WITH(NOLOCK)'
    sql_iter = f'{a_select}{a_column}{a_from}'
    not_null_count_list = pd.read_sql(sql=sql_iter, con=p_con).T.values.tolist()
    not_null_count = [n[0] for n in not_null_count_list]
    column_not_null = [c for c, n in zip(column_name, not_null_count) if n > 0]
    a_column = ',\n'.join(column_not_null) + '\n'
    column_not_null_sql = f'{a_select}{a_column}{a_from}'

    return column_not_null_sql


def convert_nan_string_to_null(string):
    string_converted = str(string).replace('nan', 'NULL')
    return string_converted


if __name__ == '__main__':
    # will not run when imported
    environment_variables()
    ConnectSQLServer
    schema_objects()
    table_column_attributes()
    quote_string()
    column_data_type()
    source_view_script()
    source_stage_table_script()
    table_column_null_check_sql()
    convert_nan_string_to_null()
