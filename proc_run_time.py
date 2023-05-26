import pandas as pd
import common_functions as cfx

source_db = r'DW_MARKETS'

server, driver, uname, pword, dsn = cfx.environment_variables('dev')

# parameter variable



sql_select = f"""

"""

server_class = cfx.ConnectSQLServer(server=server
                                    , driver=driver
                                    , uname=uname
                                    , pword=pword)

with server_class.connection(authentication='sql server', db=source_db) as conn:
    df_metro_source = pd.read_sql(sql=sql_select, con=conn)




