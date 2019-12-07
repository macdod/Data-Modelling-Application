# import pymongo

# myclient = pymongo.MongoClient("mongodb://localhost:27017/")

# mydb = myclient["test"]

# print(type(mydb.list_collection_names()))
 



# # import cx_Oracle

# # # Connect as user "hr" with password "welcome" to the "oraclepdb" service running on this computer.
# # connection = cx_Oracle.connect("root", "laddoo", "localhost/orcl.corp.amdocs.com")

# # cursor = connection.cursor()
# # cursor.execute("CREATE TABLE EMPLOYEE (id int,name VARCHAR(255))")
# # print(cursor)


from pyhive import hive
import sasl
import thrift
import thrift_sasl
import re,os,time
host = 'indlin5126'
port = 10000
username = 'bdauser'
password = 'bdauser'
database = 'test'

def hiveconnection(host_name, port, username, password, database):
	conn = hive.Connection(host = host_name, port = port, username = username, password = password, database = database, auth = 'CUSTOM')
	cor = conn.cursor()
	cur.execute("SELECT * FROM iris")
	res = cur.fetchall()

	return res

out = hiveconnection(host,port,username,password,database)
print(out)