import socketserver
import socket
import threading
import asyncio
from config import connected_clients,PORT,connected,connected_clients_timeout,client_info
from function import set_client,check_serial_exists,set_status_flag_update
from function import task2
import mysql.connector
from mysql.connector import Error

from debug import debug_print
import config

# Define the server's host and port
server_host = '192.168.100.134'
server_port = 53199


##======================================================================================
def find_row_by_serial_and_name(serial, name_setting,name_table):
    try:
        mydb = mysql.connector.connect(
            host=config.MSQL_HOST,
            user=config.MSQL_USER,
            password=config.MSQL_PASSW,
            database=config.MSQL_DATABASE
        )
        cursor = mydb.cursor()
        query = "SELECT * FROM "+name_table+" WHERE serial = %s AND name_setting = %s"
        values = (serial, name_setting)
        cursor.execute(query, values)
        result = cursor.fetchone()
        if result:
            return True
        else:
            #print("No matching record found.")
            return False
    except mysql.connector.Error as error:
        debug_print(f"Failed to fetch row: {error}")
    finally:
        # Closing database connection.
        if mydb.is_connected():
            cursor.close()
            mydb.close()
            #print("MySQL connection is closed")
##===============================================================================================================
def update_row_setting_status(serial, name_setting,var_setting, new_status,name_table):
    try:
        mydb = mysql.connector.connect(
            host=config.MSQL_HOST,
            user=config.MSQL_USER,
            password=config.MSQL_PASSW,
            database=config.MSQL_DATABASE
        )
        cursor = mydb.cursor()
        query = "UPDATE "+name_table+" SET status_setting = %s,var_setting = %s, date_time = NOW() WHERE serial = %s AND name_setting = %s"
        values = (new_status,var_setting, serial, name_setting)
        cursor.execute(query, values)
        mydb.commit()
        #print("Row updated successfully.")
    except mysql.connector.Error as error:
        debug_print(f"Failed to update row: {error}")
    finally:
        # Closing database connection.
        if mydb.is_connected():
            cursor.close()
            mydb.close()
            #print("MySQL connection is closed")
##====================================================================================================================
def insert_row_setting_status(serial, name_setting, var_setting, status_setting,name_table):
    try:
        mydb = mysql.connector.connect(
            host=config.MSQL_HOST,
            user=config.MSQL_USER,
            password=config.MSQL_PASSW,
            database=config.MSQL_DATABASE
        )
        cursor = mydb.cursor()
        query = "INSERT INTO "+name_table+ " (serial, name_setting, var_setting, status_setting) VALUES (%s, %s, %s, %s)"
        values = (serial, name_setting, var_setting, status_setting)
        cursor.execute(query, values)
        mydb.commit()
        #print("Row inserted successfully.")
    except mysql.connector.Error as error:
        debug_print(f"Failed to insert row: {error}")
    finally:
        # Closing database connection.
        if mydb.is_connected():
            cursor.close()
            mydb.close()
            #print("MySQL connection is closed")
###===============================================================================================================================================
def get_frame(message,client_id):
    values = {}

    for substring in message.split("#"):
        if substring != "" and substring != "CRC" and substring != "END":
            index, value = substring.split(":")
            values[index] = str(value)
        else:
            break

            
    if check_serial_exists(client_id,config.MSQL_TABLE_STATUS) == False:
        
        try:
            mydb = mysql.connector.connect(host=config.MSQL_HOST,user=config.MSQL_USER,password=config.MSQL_PASSW,database=config.MSQL_DATABASE)
            cursor = mydb.cursor()
            sql_insert_query = "INSERT INTO "+config.MSQL_TABLE_STATUS +" (serial,info,status) VALUES (%s,%s,%s)"
            infor_temp = "" + client_info[client_id]['ip'] + ":" + str(client_info[client_id]['port']) + ""
            status_temp = "online"
            record_to_insert = (client_id,infor_temp,status_temp)
            cursor.execute(sql_insert_query, record_to_insert)
            mydb.commit()
            debug_print("Record inserted successfully")
        except mysql.connector.Error as error:
            debug_print(f"Failed to insert into MySQL table {error}")   
        finally:
            # closing database connection.
            if mydb.is_connected():
               cursor.close()
               mydb.close()
               debug_print("MySQL connection is closed")
    else:
        try:
            mydb = mysql.connector.connect(host=config.MSQL_HOST,user=config.MSQL_USER,password=config.MSQL_PASSW,database=config.MSQL_DATABASE)
            cursor = mydb.cursor()
            sql_insert_query = "UPDATE "+config.MSQL_TABLE_STATUS +" SET info = %s, status = %s WHERE serial = %s"
            infor_temp = "" + client_info[client_id]['ip'] + ":" + str(client_info[client_id]['port']) + ""
            status_temp = "online"
            val = (infor_temp,status_temp,client_id)
            cursor.execute(sql_insert_query, val)
            mydb.commit()
            debug_print("Record inserted successfully")
        except mysql.connector.Error as error:
            debug_print(f"Failed to insert into MySQL table {error}")   
        finally:
            # closing database connection.
            if mydb.is_connected():
               cursor.close()
               mydb.close()
               debug_print("MySQL connection is closed")
    # for reg info if reg_2 = 0.
    if values['2'] == '0':
        debug_print("mess 0: reg info")
               
        if check_serial_exists(client_id,config.MSQL_TABLE_INFO) == False:
        
            try:
                mydb = mysql.connector.connect(host=config.MSQL_HOST,user=config.MSQL_USER,password=config.MSQL_PASSW,database=config.MSQL_DATABASE)
                cursor = mydb.cursor()
                reg_ ="reg_"
                temp_mes = "INSERT INTO "+config.MSQL_TABLE_INFO +" (serial,"
                j =0
                for i, value in values.items():
                    j+=1
                    if int(i) == 0:
                        continue
                    if int(j) == int(len(values)):
                        temp_mes= temp_mes + reg_+str(i)+")"
                    else: 
                        temp_mes= temp_mes + reg_+str(i)+ ","

                temp_mes1 =  " VALUES (\'"+str(client_id)+"\',"
                j =0
                for i, value in values.items():
                    j+=1
                    if int(i) == 0:
                        continue
                    if int(j) == int(len(values)):
                        temp_mes1= temp_mes1 +"\'"+str(value)+"\')"
                    else: 
                        temp_mes1= temp_mes1 +"\'"+str(value)+"\',"

                sql_insert_query =temp_mes+temp_mes1
                cursor.execute(sql_insert_query)
                mydb.commit()
                debug_print("Record inserted successfully")
            except mysql.connector.Error as error:
                debug_print(f"Failed to insert into MySQL table {error}")   
            finally:
            # closing database connection.
                if mydb.is_connected():
                    cursor.close()
                    mydb.close()
                    debug_print("MySQL connection is closed")
        else:
            try:
                mydb = mysql.connector.connect(host=config.MSQL_HOST,user=config.MSQL_USER,password=config.MSQL_PASSW,database=config.MSQL_DATABASE)
                cursor = mydb.cursor()
                reg_ ="reg_"
                temp_mes = "UPDATE "+config.MSQL_TABLE_INFO +" SET "
                j =0
                for i, value in values.items():
                    j+=1
                    if int(i) == 0:
                        continue
                    if int(j) == int(len(values)):
                        temp_mes= temp_mes + reg_+str(i)+ " = "+"\'"+str(value)+"\' "
                    else: 
                        temp_mes= temp_mes + reg_+str(i)+ " = "+"\'"+str(value)+"\', "

                sql_insert_query = temp_mes + ", date_time = NOW() WHERE serial = %s"
                val = (client_id,)
                cursor.execute(sql_insert_query, val)
                mydb.commit()
                debug_print("Record inserted successfully")
            except mysql.connector.Error as error:
                debug_print(f"Failed to insert into MySQL table {error}")   
            finally:
                # closing database connection.
                if mydb.is_connected():
                    cursor.close()
                    mydb.close()
                    debug_print("MySQL connection is closed")
    #for setting info. if reg_2 = 1.       
    elif values['2'] == '1':
        debug_print("mess 1: reg setting info ")
               
        if check_serial_exists(client_id,config.MSQL_TABLE_SETTING_INFO) == False:
        
            try:
                mydb = mysql.connector.connect(host=config.MSQL_HOST,user=config.MSQL_USER,password=config.MSQL_PASSW,database=config.MSQL_DATABASE)
                cursor = mydb.cursor()
                reg_ ="reg_"
                temp_mes = "INSERT INTO "+config.MSQL_TABLE_SETTING_INFO +" (serial,"
                j =0
                for i, value in values.items():
                    j+=1
                    if int(i) == 0:
                        continue
                    if int(j) == int(len(values)):
                        temp_mes= temp_mes + reg_+str(i)+")"
                    else: 
                        temp_mes= temp_mes + reg_+str(i)+ ","

                temp_mes1 =  " VALUES (\'"+str(client_id)+"\',"
                j =0
                for i, value in values.items():
                    j+=1
                    if int(i) == 0:
                        continue
                    if int(j) == int(len(values)):
                        temp_mes1= temp_mes1 +"\'"+str(value)+"\')"
                    else: 
                        temp_mes1= temp_mes1 +"\'"+str(value)+"\',"

                sql_insert_query =temp_mes+temp_mes1
                cursor.execute(sql_insert_query)
                mydb.commit()
                debug_print("Record inserted successfully")
            except mysql.connector.Error as error:
                debug_print(f"Failed to insert into MySQL table {error}")   
            finally:
            # closing database connection.
                if mydb.is_connected():
                    cursor.close()
                    mydb.close()
                    debug_print("MySQL connection is closed")
        else:
            try:
                mydb = mysql.connector.connect(host=config.MSQL_HOST,user=config.MSQL_USER,password=config.MSQL_PASSW,database=config.MSQL_DATABASE)
                cursor = mydb.cursor()
                reg_ ="reg_"
                temp_mes = "UPDATE "+config.MSQL_TABLE_SETTING_INFO +" SET "
                j =0
                for i, value in values.items():
                    j+=1
                    if int(i) == 0:
                        continue
                    if int(j) == int(len(values)):
                        temp_mes= temp_mes + reg_+str(i)+ " = "+"\'"+str(value)+"\' "
                    else: 
                        temp_mes= temp_mes + reg_+str(i)+ " = "+"\'"+str(value)+"\', "

                sql_insert_query = temp_mes + ", date_time = NOW() WHERE serial = %s"
                val = (client_id,)
                cursor.execute(sql_insert_query, val)
                mydb.commit()
                debug_print("Record inserted successfully")
            except mysql.connector.Error as error:
                debug_print(f"Failed to insert into MySQL table {error}")   
            finally:
                # closing database connection.
                if mydb.is_connected():
                    cursor.close()
                    mydb.close()
                    debug_print("MySQL connection is closed")
               
    debug_print(values)
    #print(values)

###==============================================================================================================================================
def get_frame_return_setting(message):
    values = {}
    serial_number, message = message[2:].split('#', 1)

    for substring in message.split("#"):
        if substring != "" and substring != "CRC" and substring != "END":
            index, value = substring.split(":")
            values[index] = str(value)
        else :
            break

    if values['1'] == "flag_update_solar":

      if check_serial_exists(serial_number,config.MSQL_TABLE_SETTING_UPDATE) == True:
        if values['2'] == "true":
            status_temp = "state_successfully"
        else:
            status_temp = "state_setting_false"

        set_status_flag_update(serial_number,status_temp)
    else :
        if find_row_by_serial_and_name(serial_number,values['1'],config.MSQL_TABLE_SETTING_STATUS) == True:
            if values['2'] == "true":
                status_temp = "state_successfully"
                update_row_setting_status(serial_number,values['1'],values['3'],status_temp,config.MSQL_TABLE_SETTING_STATUS)
            else:
                status_temp = "state_setting_false"
                update_row_setting_status(serial_number,values['1'],values['3'],status_temp,config.MSQL_TABLE_SETTING_STATUS)
        else:
            if values['2'] == "true":
                status_temp = "state_successfully"
                insert_row_setting_status(serial_number,values['1'],values['3'],status_temp,config.MSQL_TABLE_SETTING_STATUS)
            else:
                status_temp = "state_setting_false"
                insert_row_setting_status(serial_number,values['1'],values['3'],status_temp,config.MSQL_TABLE_SETTING_STATUS)
###======================================================================================================
###======================================================================================================
async def set_offline(client_id):
    if check_serial_exists(client_id,config.MSQL_TABLE_STATUS) == False:
        debug_print("check serial exists false")
    else:
        try:
            mydb = mysql.connector.connect(host=config.MSQL_HOST,user=config.MSQL_USER,password=config.MSQL_PASSW,database=config.MSQL_DATABASE)
            cursor = mydb.cursor()
            sql_insert_query = "UPDATE "+config.MSQL_TABLE_STATUS +" SET status = %s WHERE serial = %s"
            #infor_temp = "" + client_info[client_id]['ip'] + ":" + str(client_info[client_id]['port']) + ""
            status_temp = "offline"
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

def set_offline_1(client_id):
    if check_serial_exists(client_id,config.MSQL_TABLE_STATUS) == False:
        debug_print("check serial exists false")
    else:
        try:
            mydb = mysql.connector.connect(host=config.MSQL_HOST,user=config.MSQL_USER,password=config.MSQL_PASSW,database=config.MSQL_DATABASE)
            cursor = mydb.cursor()
            sql_insert_query = "UPDATE "+config.MSQL_TABLE_STATUS +" SET status = %s WHERE serial = %s"
            #infor_temp = "" + client_info[client_id]['ip'] + ":" + str(client_info[client_id]['port']) + ""
            status_temp = "offline"
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

###======================================================================================================

async def set_online(client_id):
    if check_serial_exists(client_id,config.MSQL_TABLE_STATUS) == False:
        debug_print("check serial exists false")
    else:
        try:
            mydb = mysql.connector.connect(host=config.MSQL_HOST,user=config.MSQL_USER,password=config.MSQL_PASSW,database=config.MSQL_DATABASE)
            cursor = mydb.cursor()
            sql_insert_query = "UPDATE "+config.MSQL_TABLE_STATUS +" SET status = %s WHERE serial = %s"
            #infor_temp = "" + client_info[client_id]['ip'] + ":" + str(client_info[client_id]['port']) + ""
            status_temp = "offline"
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
###=====================================================================================================
def get_status(client_id):
    if check_serial_exists(client_id,config.MSQL_TABLE_STATUS) == False:
        debug_print("check serial exists false")
    else:
        try:
            mydb = mysql.connector.connect(host=config.MSQL_HOST,user=config.MSQL_USER,password=config.MSQL_PASSW,database=config.MSQL_DATABASE)
            cursor = mydb.cursor()
            sql_insert_query = "SELECT status FROM "+config.MSQL_TABLE_STATUS +" WHERE serial=%s"

            #cursor.execute("SELECT status FROM "+ config.MSQL_TABLE_STATUS+"WHERE serial=%s", (client_id,))
            val = (client_id,)
            cursor.execute(sql_insert_query, val)
            row = cursor.fetchone()

            if row is not None:
                return row[0]
            else:
                debug_print("Serial number {} not found in the database".format(client_id))

            debug_print("get_status")
        except mysql.connector.Error as error:
            debug_print(f"Failed to insert into MySQL table {error}")   
        finally:
            # closing database connection.
            if mydb.is_connected():
               cursor.close()
               mydb.close()
               #print("MySQL connection is closed")
###=========================================================================================================
async def check_status():
    while True:

        try:
            mydb = mysql.connector.connect(host=config.MSQL_HOST,user=config.MSQL_USER,password=config.MSQL_PASSW,database=config.MSQL_DATABASE)
            cursor = mydb.cursor()
            sql_insert_query = "SELECT serial FROM "+config.MSQL_TABLE_STATUS
            cursor.execute(sql_insert_query)
            for row in cursor.fetchall():
               #print(row[0])
               
                if row[0] in connected_clients:
                    for conn in connected:
                        if connected_clients[row[0]] == conn:
                            #set online
                            if get_status(row[0])== "offline":
                                await set_online(row[0])
                                debug_print("set online "+row[0])       
                else:
                    #set offline
                    if get_status(row[0])== "online":
                        await set_offline(row[0])
                        debug_print("set offline "+row[0])         
            #mydb.commit()
            #print("Record inserted successfully")
        except mysql.connector.Error as error:
            debug_print(f"Failed to insert into MySQL table {error}")   
        finally:
            # closing database connection.
            if mydb.is_connected():
               cursor.close()
               mydb.close()
               #print("MySQL connection is closed")S

        await asyncio.sleep(1)#0.05
# Create a custom handler by subclassing the BaseRequestHandler class
#==========================================================================
async def timeout_clients():
    task2_cnt = 0
    while True:
        #print("Task 2 running")
        task2_cnt+=1
        await asyncio.sleep(1)
async def timeout_task( self):
    timeout = 60  # Specify the timeout value in seconds

    try:
        await asyncio.sleep(timeout)
    except asyncio.CancelledError:
        # Timeout task was cancelled due to data reception
        return
    # Timeout occurred, close the client connection
    print(f"Timeout occurred for client {self.client_ip}:{self.client_port}")
    self.transport.close()     

class MyTCPHandler(asyncio.Protocol):
    def __init__(self):
        super().__init__()
        self.data = None
        self.message = None
        self.client_ip = None
        self.client_port = None
        self.client_id = None
        self.timeout_task = None

    def connection_made(self, transport):
        self.transport = transport
        self.client_ip, self.client_port = transport.get_extra_info("peername")
        print(f"Client {self.client_ip}:{self.client_port} connected")
        connected.add(self)
        # Start the timeout task
        self.timeout_task = asyncio.create_task(timeout_task(self))

    def connection_lost(self, exc):
        print(f"Client {self.client_ip}:{self.client_port} disconnected")
        if self.client_id in connected_clients:
            if connected_clients[self.client_id] == self:
                #set_offline(self.client_id)
                asyncio.create_task(set_offline(self.client_id))

                del connected_clients[self.client_id]
                del client_info[self.client_id]
        connected.discard(self)
        self.transport.close()
        if self.timeout_task is not None and not self.timeout_task.done():
            self.timeout_task.cancel()
       

    def data_received(self, data):
        # Data received, cancel the timeout task
        #self.timeout_task = asyncio.create_task(timeout_task(self))
        self.process_data(data)
        if self.timeout_task is not None and not self.timeout_task.done():
            self.timeout_task.cancel()
            self.timeout_task = None
            self.timeout_task = asyncio.create_task(timeout_task(self))
        

    def process_data(self, data):

        # Reset the timeout task upon receiving data

        message = data.decode().strip()
        
        if message.startswith('@SOLAR'):    
            #get time for check time out
            #await get_time(websocket)
            #check message
            serial_number, message = message[1:].split('#', 1)
            self.client_id = str(serial_number)
                    
            if self.client_id in connected_clients:
                if connected_clients[self.client_id] != self:
                    self.transport.close()
                else:
                    response = f'@{self.client_id} is connected; Server solar version: {config.SERVER_WS_VERSION}'
                    self.transport.write(response.encode())
                    client_info[self.client_id]={'ip':self.client_ip,'port':self.client_port} 
                    get_frame(message, self.client_id)
                                   
                                
            else:
                # Add a new entry to the dictionary
                connected_clients[self.client_id] = self
                client_info[self.client_id]={'ip':self.client_ip,'port':self.client_port}
                response = f'@{self.client_id} is connected; Server solar version: {config.SERVER_WS_VERSION}'
                self.transport.write(response.encode())
                get_frame(message,self.client_id)
                                   
                    # Broadcast the message to all connected clients.
        elif  message.startswith('@@SOLAR'):
            #get_frame
            get_frame_return_setting(message)
            #for scheduler.
        elif  message.startswith('@@@SOLAR'):
            #await  get_frame_return_scheduler(message)
            test =2
        else:
            test =3
            #self.request.close()      
      

async def start_server():
    server = await asyncio.get_running_loop().create_server(
        MyTCPHandler,
        server_host,
        server_port
    )

    async with server:
        await server.serve_forever()


async def main():
    debug_print("Server listening on Port " + str(server_port))
    #await mysql_init_python()
    
    server_task = asyncio.create_task(start_server())

    t1 = asyncio.create_task(set_client())
    t2 = asyncio.create_task(task2())
    timeout_task = asyncio.create_task(timeout_clients())
    check_status_task = asyncio.create_task(check_status())

    # Wait for all tasks to complete
    await asyncio.gather(server_task, t1,t2, timeout_task, check_status_task)


if __name__ == "__main__":
    asyncio.run(main())


