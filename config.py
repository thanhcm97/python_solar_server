#task===============================================
set_client_cnt = 0
task2_cnt = 0

#web socket=========================================
connected_clients = {}
connected_clients_timeout = {}
client_info={}
# A set of connected ws clients
connected = set()

# Server data=======================================
PORT = 53198
BUFFER_REV = 5000
TIME_OUT_CLIENT =60# 2 min
TIME_OUT_SETTING_UPDATE =10

#msql===============================================
MSQL_HOST="localhost"
MSQL_USER="root"
MSQL_PASSW="ems@admin~!@#$88"
MSQL_DATABASE="solar_database"
MSQL_TABLE_STATUS ="solar_statuss"
MSQL_TABLE_INFO   ="solar_infos"
MSQL_TABLE_SETTING_INFO = "solar_setting_infos"
MSQL_TABLE_SETTING_UPDATE ="solar_setting_updates"
MSQL_TABLE_SETTING_TEMP= "solar_setting_temps"
MSQL_TABLE_SETTING_STATUS= "solar_setting_statuss"
#MSQL_TABLE_SCHEDULER_INFO = "ems_table_scheduler_info"

#debug
DEBUG_MAX_LINE=100000
#
SERVER_WS_VERSION="V1.1"
flag = False