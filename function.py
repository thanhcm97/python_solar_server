import asyncio
from config import set_client_cnt,task2_cnt
from config import connected_clients,PORT,connected,flag
import config
import mysql.connector
from mysql.connector import Error
from debug import debug_print
import time



###=================================================================
# Create a connection to the MySQL server
mydb = None
cursor = None
mes_set_flag_update = ""

###=================================================================

async def send_event_to_all_clients(event):
    for client_id, websocket in connected_clients.items():
        await websocket.send(event)

###===================================================================
def check_serial_exists(serial,table_name):
    try:
 
        mydb = mysql.connector.connect(host=config.MSQL_HOST,user=config.MSQL_USER,password=config.MSQL_PASSW,database=config.MSQL_DATABASE)
        cursor = mydb.cursor()
        query = "SELECT * FROM "+ table_name +" WHERE serial = %s"
        cursor.execute(query, (serial,))
        row = cursor.fetchone()
        if row is not None:
            return True
        else:
            return False
    except mysql.connector.Error as error:
        debug_print(f"Failed to check serial"+table_name+" {error}")   
    finally:
    # closing database connection.
        if mydb.is_connected():
            cursor.close()
            mydb.close()
            #print("MySQL connection is closed")
##====================================================================
def check_id_table_exists(id,table_name):
    try:
 
        mydb = mysql.connector.connect(host=config.MSQL_HOST,user=config.MSQL_USER,password=config.MSQL_PASSW,database=config.MSQL_DATABASE)
        cursor = mydb.cursor()
        query = "SELECT * FROM "+ table_name +" WHERE id = %s"
        cursor.execute(query, (id,))
        row = cursor.fetchone()
        if row is not None:
            return True
        else:
            return False
    except mysql.connector.Error as error:
        debug_print(f"Failed to check id"+table_name+" {error}")   
    finally:
    # closing database connection.
        if mydb.is_connected():
            cursor.close()
            mydb.close()
            #print("MySQL connection is closed")
    
##====================================================================


def get_status_flag_update(client_id):
    if check_serial_exists(client_id,config.MSQL_TABLE_SETTING_UPDATE) == False:
        debug_print("check serial exists false"+ config.MSQL_TABLE_SETTING_UPDATE)
        return "exists_false"
    else:
        try:
            mydb = mysql.connector.connect(host=config.MSQL_HOST,user=config.MSQL_USER,password=config.MSQL_PASSW,database=config.MSQL_DATABASE)
            cursor = mydb.cursor()
            sql_insert_query = "SELECT name_setting, var_setting, status_setting FROM "+config.MSQL_TABLE_SETTING_UPDATE +" WHERE serial=%s"

            #cursor.execute("SELECT status FROM "+ config.MSQL_TABLE_STATUS+"WHERE serial=%s", (client_id,))
            val = (client_id,)
            cursor.execute(sql_insert_query, val)
            result = cursor.fetchone()
            if result is not None:
                return result # Unpack the result into variables
            else:
                debug_print("No matching record found")
        except mysql.connector.Error as error:
            debug_print(f"Failed to insert into MySQL table {error}")   
        finally:
            # closing database connection.
            if mydb.is_connected():
               cursor.close()
               mydb.close()
               #print("MySQL connection is closed")    

###=============================================================================
def get_status_setting(id_table):

    if check_id_table_exists(id_table,config.MSQL_TABLE_SETTING_TEMP) == False:
        debug_print("check id table false"+ config.MSQL_TABLE_SETTING_TEMP)
        return "exists_false"
    else:
        try:
            mydb = mysql.connector.connect(host=config.MSQL_HOST,user=config.MSQL_USER,password=config.MSQL_PASSW,database=config.MSQL_DATABASE)
            cursor = mydb.cursor()
            sql_insert_query = "SELECT serial, name_setting, var_setting, status_setting FROM "+config.MSQL_TABLE_SETTING_TEMP +" WHERE id=%s"

            #cursor.execute("SELECT status FROM "+ config.MSQL_TABLE_STATUS+"WHERE serial=%s", (client_id,))
            val = (id_table,)
            cursor.execute(sql_insert_query, val)
            result = cursor.fetchone()
            if result is not None:
                return result # Unpack the result into variables
            else:
                debug_print("No matching record found")
        except mysql.connector.Error as error:
            debug_print(f"Failed to insert into MySQL table {error}")   
        finally:
            # closing database connection.
            if mydb.is_connected():
               cursor.close()
               mydb.close()
               #print("MySQL connection is closed")


###==============================================================================
def delete_row_in_table(id_table, table_name):
    if check_id_table_exists(id_table,table_name) == True:
        try:
            mydb = mysql.connector.connect(host=config.MSQL_HOST,user=config.MSQL_USER,password=config.MSQL_PASSW,database=config.MSQL_DATABASE)
            cursor = mydb.cursor()
            query= "DELETE FROM " + table_name + " WHERE id=%s"
            cursor.execute(query, (id_table,))
            mydb.commit()
        except mysql.connector.Error as error:
            debug_print(f"Failed to delete from id in "+table_name+" {error}")   
        finally:
            # closing database connection.
            if mydb.is_connected():
                cursor.close()
                mydb.close()
                #print("MySQL connection is closed")
        


###=====================================================================================================================================================
def set_status_flag_update(client_id,state):
    if check_serial_exists(client_id,config.MSQL_TABLE_SETTING_UPDATE) == False:
        debug_print("check serial exists false")
    else:
        try:
            mydb = mysql.connector.connect(host=config.MSQL_HOST,user=config.MSQL_USER,password=config.MSQL_PASSW,database=config.MSQL_DATABASE)
            cursor = mydb.cursor()
            sql_insert_query = "UPDATE "+config.MSQL_TABLE_SETTING_UPDATE +" SET status_setting = %s WHERE serial = %s"
            #infor_temp = "" + client_info[client_id]['ip'] + ":" + str(client_info[client_id]['port']) + ""
            status_temp = state
            val = (status_temp,client_id)
            cursor.execute(sql_insert_query, val)
            mydb.commit()
            #print("Record inserted successfully")
        except mysql.connector.Error as error:
            debug_print(f"Failed to insert into MySQL table {error}")   
        finally:
            # closing database connection.
            if mydb.is_connected():
               cursor.close()
               mydb.close()
               #print("MySQL connection is closed")
###=====================================================================================================================================================
async def set_client() :
    global set_client_cnt
    global flag 
    set_client_cnt =0
    while True:
        set_client_cnt+=1
        # setting for update=======================================================================================================================
        try:
            mydb = mysql.connector.connect(host=config.MSQL_HOST,user=config.MSQL_USER,password=config.MSQL_PASSW,database=config.MSQL_DATABASE)
            cursor = mydb.cursor()
            sql_insert_query = "SELECT serial FROM "+config.MSQL_TABLE_SETTING_UPDATE
            cursor.execute(sql_insert_query)
            for row in cursor.fetchall():
                name_setting, var_setting, status_setting = get_status_flag_update(row[0])

                if status_setting == "state_init":
                    if row[0] in connected_clients:
                        for conn in connected:
                            if connected_clients[row[0]] == conn:
                                #send flag update
                                try:
                                    if name_setting is not None and var_setting is not None:
                                        mes_temp = "#" + name_setting + ":" + var_setting + "#CRC#END"
                                        mes_set_flag_update = "@@"+ row[0] +"#0:" + str(len(mes_temp)) + mes_temp

                                        set_status_flag_update(row[0],"state_setting...")
                                        #await conn.send(mes_set_flag_update)
                                        #response = f'@{client_id} is connected; Server solar version: {config.SERVER_WS_VERSION}'
                                        conn.transport.write(mes_set_flag_update.encode())
                                        await asyncio.sleep(1)
                                        start_timeout = asyncio.get_event_loop().time()
                                        while True:
                                            name_setting, var_setting, status_setting = get_status_flag_update(row[0])
                                            if status_setting == "state_successfully" :
                                                break
                                            end_timeout = asyncio.get_event_loop().time()
                                            if end_timeout - start_timeout > config.TIME_OUT_SETTING_UPDATE:
                                                set_status_flag_update(row[0],"state_setting_false")
                                                break
                                            await asyncio.sleep(1)
                                    else :
                                     debug_print("variable is NULL") 
                                 
                                except TypeError as e:
                                    debug_print("Error occurred:", e) 
                                        
        except mysql.connector.Error as error:
            debug_print(f"Failed to insert into MySQL table {error}")   
        finally:
            # closing database connection.
            if mydb.is_connected():
               cursor.close()
               mydb.close()
               #print("MySQL connection is closed")

        # for other setting==========================================================================================================================
        try:
            mydb = mysql.connector.connect(host=config.MSQL_HOST,user=config.MSQL_USER,password=config.MSQL_PASSW,database=config.MSQL_DATABASE)
            cursor = mydb.cursor()
            sql_insert_query = "SELECT id FROM "+config.MSQL_TABLE_SETTING_TEMP
            cursor.execute(sql_insert_query)
            for row in cursor.fetchall():
                serial,name_setting, var_setting, status_setting = get_status_setting(row[0])

                if status_setting == "state_init":
                    if serial in connected_clients:
                        for conn in connected:
                            if connected_clients[serial] == conn:
                                #send flag update
                                try:
                                    if name_setting is not None and var_setting is not None:
                                        mes_temp = "#" + name_setting + ":" + var_setting + "#CRC#END"
                                        mes_setting = "@@"+ serial +"#0:" + str(len(mes_temp)) + mes_temp
 
                                        #await conn.send(mes_setting)
                                        conn.transport.write(mes_setting.encode())
                                        await asyncio.sleep(1)
                                    else :
                                        await asyncio.sleep(1)
                                        test =1
                                except TypeError as e:
                                    debug_print("Error occurred:", e)  
                                delete_row_in_table(row[0],config.MSQL_TABLE_SETTING_TEMP)   
                else:
                    delete_row_in_table(row[0],config.MSQL_TABLE_SETTING_TEMP)

        except mysql.connector.Error as error:
            debug_print(f"Failed to insert into MySQL table {error}")   
        finally:
            # closing database connection.
            if mydb.is_connected():
               cursor.close()
               mydb.close()
               #print("MySQL connection is closed")
        await asyncio.sleep(0.5)
    
        
async def task2():
    global task2_cnt
    task2_cnt = 0
    while True:
        #print("Task 2 running")
        task2_cnt+=1
        await asyncio.sleep(1)