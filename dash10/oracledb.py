import cx_Oracle
import numpy as np

 

######  Output Table Names
def gettablenames(host,port,user,passwd,dbname):
	connection = cx_Oracle.connect(user,passwd, host+"/orcl.corp.amdocs.com")
	mycursor = connection.cursor()
	mycursor.execute("SELECT table_name FROM user_tables")
	myresult = mycursor.fetchall()
	res = []
	for x in myresult:
		res.append(x[0])
	return res




######  Output Data of given Columns of a Table
def getdata(host,port,user,passwd,dbname,tb,cols):
	connection = cx_Oracle.connect(user, passwd, host+"/orcl.corp.amdocs.com")
	mycursor = connection.cursor()
	s = "SELECT "+cols[0]
	for i in range(1,len(cols)):
		s += ","
		s += cols[i]
	s += " FROM "+tb

	mycursor.execute(s)

	myresult = mycursor.fetchall()
	res = np.transpose(myresult)
	return res




######  Output Column Names of a Table
def getcols(host,port,user,passwd,dbname,tb):
	connection = cx_Oracle.connect(user, passwd, host+"/orcl.corp.amdocs.com")
	mycursor = connection.cursor()
	mycursor.execute("SELECT column_name,data_type FROM all_tab_cols WHERE table_name = '"+tb.upper() + "'")
	myresult = mycursor.fetchall()
	res = []
	colname = []
	dt = []
	types = []
	for x in myresult:
		colname.append(x[0])
		dt.append(x[1])
		types.append('')

	res.append(colname)
	res.append(dt)
	res.append(types)
	return res





###### Join Two Table and Output Column names of the Joined Table
def jointable(host,port,user,passwd,dbname,tb1,tb2,col1,col2):
	connection = cx_Oracle.connect(user, passwd, host+"/orcl.corp.amdocs.com")
	data1 = getcols(host,port,user,passwd,dbname,tb1)
	data2 = getcols(host,port,user,passwd,dbname,tb2)
	res = []
	s = "CREATE OR REPLACE VIEW view1 as SELECT ";
	s += "t1."+data1[0]
	s += " as t1_";
	s += data1[0]
	res.append("t1_"+data1[0])
	for i in range(1,len(data1)):
		s += ",t1.";
		s += data1[i]
		s += " as t1_";
		s += data1[i]
		res.append("t1_"+data1[i])
	for i in range(0,len(data2)):
		s += ",t2.";
		s += data2[i]
		s += " as t2_";
		s += data2[i]
		res.append("t2_"+data2[i])

	s += " FROM "+tb1+" t1,"+tb2+" t2 WHERE t1."+col1 +"= t2."+col2;
	mycursor = connection.cursor()
	mycursor.execute(s)
	return res

