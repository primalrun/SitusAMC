import pyodbc


def connect_sql_server(login_dict):  # returns sql server connection
    server = login_dict['server']
    database = login_dict['database']
    if login_dict['auth'] == 'windows':
        conn_str = (
            r'Driver={ODBC Driver 17 for SQL Server};'
            f'Server={server};'
            f'Database={database};'
            r'Trusted_Connection=yes;'
        )
    else:
        username = login_dict['username']
        password = login_dict['password']
        conn_str = (
            r'Driver={ODBC Driver 17 for SQL Server};'
            f'Server={server};'
            f'Database={database};'
            f'UID={username};'
            f'PWD={password};'
        )
    return pyodbc.connect(conn_str)
