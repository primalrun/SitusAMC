import common_functions as cfx
import os

source_file_dir = r'C:\Users\jasonwalker\OneDrive - SitusAMC\Documents\project\ADO\DEPLOY'
source_db = 'DW_SAMC_INTERNAL'
server, driver, uname, pword, dsn = cfx.environment_variables('uat')

# order , file name
deploy_file_names = [
[1, 'DATA_SOURCE_LOAD.sql']
,[4, 'dbo^DIM_ADO_LOGGED_ACTIVITY_TYPE.sql']
,[4, 'dbo^DIM_ADO_USER.sql']
,[4, 'dbo^DIM_ADO_WORK_ITEM_STATE.sql']
,[4, 'dbo^DIM_ADO_WORK_ITEM_TYPE.sql']
,[4, 'dbo^FACT_ADO_DEVELOPER_HOURS.sql']
,[4, 'dbo^FACT_ADO_JOB_BOARD.sql']
,[4, 'dbo^FACT_ADO_USER_STORY_DEVELOPER_ACTIVITY.sql']
,[5, 'dbo^vw_DIM_ADO_LOGGED_ACTIVITY_TYPE.sql']
,[5, 'dbo^vw_DIM_ADO_USER.sql']
,[5, 'dbo^vw_DIM_ADO_WORK_ITEM_STATE.sql']
,[5, 'dbo^vw_DIM_ADO_WORK_ITEM_TYPE.sql']
,[5, 'dbo^vw_FACT_ADO_DEVELOPER_HOURS.sql']
,[5, 'dbo^vw_FACT_ADO_FACT_ADO_JOB_BOARD.sql']
,[5, 'dbo^vw_FACT_ADO_USER_STORY_DEVELOPER_ACTIVITY.sql']
,[8, 'stage^ADO_DIM_VALIDATION.sql']
,[3, 'stage^ADO_HOURS_WORKED.sql']
,[3, 'stage^ADO_STATE_CHANGE.sql']
,[3, 'stage^ADO_TAG_CHANGE.sql']
,[3, 'stage^ADO_TAG_SPLIT.sql']
,[3, 'stage^ADO_USER_STORY_AND_CHILD_WORK_ITEMS.sql']
,[3, 'stage^ADO_USER_STORY_WORK_ITEMS.sql']
,[7, 'stage^DE_LOAD_ADO_DATA.sql']
,[6, 'stage^DE_LOAD_ADO_USER_STORY_WORK_ITEMS.sql']
,[6, 'stage^DE_LOAD_DIM_ADO_LOGGED_ACTIVITY_TYPE.sql']
,[6, 'stage^DE_LOAD_DIM_ADO_USER.sql']
,[6, 'stage^DE_LOAD_DIM_ADO_WORK_ITEM_STATE.sql']
,[6, 'stage^DE_LOAD_DIM_ADO_WORK_ITEM_TYPE.sql']
,[6, 'stage^DE_LOAD_FACT_ADO_DEVELOPER_HOURS.sql']
,[6, 'stage^DE_LOAD_FACT_ADO_JOB_BOARD.sql']
,[6, 'stage^DE_LOAD_FACT_ADO_USER_STORY_DEVELOPER_ACTIVITY.sql']
,[6, 'stage^DE_LOAD_STAGE_ADO_DATA.sql']
,[2, 'stage^SRC_WorkItems.sql']
,[2, 'stage^SRC_WorkItemsHierarchy.sql']
,[2, 'stage^SRC_WorkItemsRevision.sql']
,[2, 'stage^SRC_WorkLogs.sql']
,[2, 'stage^vw_SRC_WorkItems.sql']
,[2, 'stage^vw_SRC_WorkItemsHierarchy.sql']
,[2, 'stage^vw_SRC_WorkItemsRevision.sql']
,[2, 'stage^vw_SRC_WorkLogs.sql']
]

file_names_sorted = sorted(deploy_file_names, key=lambda x: x[0])
files_to_deploy = [os.path.join(source_file_dir, f[1]) for f in file_names_sorted]

server_class = cfx.ConnectSQLServer(server=server
                                    , driver=driver
                                    , uname=uname
                                    , pword=pword)

with server_class.connection(authentication='windows', db=source_db) as conn:
    cursor = conn.cursor()

    for file in files_to_deploy:
        with open(file, mode='r') as f:
            sql = f.read()
        cursor.execute(sql)

print('deployment complete')

