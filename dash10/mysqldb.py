import mysql.connector
import pandas as pd
import numpy as np
from . import excel
from sqlalchemy import create_engine

###### Check database connectivity
def checkdatabase(host , port, user, passwd, dbname):
	conn = mysql.connector.connect(
		host=host,
		user=user,
		password=passwd, 
		database=dbname
	)
	if conn.is_connected():
		return True
	else:
		return False

######  Output Table Names
def gettablenames(host,port,user,passwd,dbname):
	mydb = mysql.connector.connect(
	  host=host,
	  user=user,
	  passwd=passwd,
	  database=dbname
	)
	mycursor = mydb.cursor()
	mycursor.execute("SELECT table_name FROM information_schema.tables where table_type not like 'view' and table_schema='"+dbname+"'")
	myresult = mycursor.fetchall()
	res = []
	for x in myresult:
		res.append(x[0])
	return res


######  Output Table Names
def getviewnameswithrep(host,port,user,passwd,dbname):
	mydb = mysql.connector.connect(
	  host=host,
	  user=user,
	  passwd=passwd,
	  database=dbname
	)
	mycursor = mydb.cursor()
	mycursor.execute("SELECT table_name FROM information_schema.tables where table_type like 'view' and table_schema like '"+dbname+"' ")
	myresult = mycursor.fetchall()
	res = []
	for x in myresult:
		if 'rep$' in x[0] and x[0] != 'view1' :
			res.append(x[0][4:])
	return res

def getviewnameswithoutrep(host,port,user,passwd,dbname):
	mydb = mysql.connector.connect(
	  host=host,
	  user=user,
	  passwd=passwd,
	  database=dbname
	)
	mycursor = mydb.cursor()
	mycursor.execute("SELECT table_name FROM information_schema.tables where table_type like 'view' and table_schema like '"+dbname+"' ")
	myresult = mycursor.fetchall()
	res = []
	for x in myresult:
		if 'rep$' not in x[0] and x[0] != 'view1' :
			res.append(x[0])
	return res


# def getviewnames(host,port,user,passwd,dbname):
# 	mydb = mysql.connector.connect(
# 	  host=host,
# 	  user=user,
# 	  passwd=passwd,
# 	  database=dbname
# 	)
# 	mycursor = mydb.cursor()
# 	mycursor.execute("SELECT table_name FROM information_schema.tables where table_type like 'view' and table_schema like '"+dbname+"' ")
# 	myresult = mycursor.fetchall()
# 	res = []
# 	for x in myresult:
# 		res.append(x[0])
# 	return res




######  Output Data of given Columns of a Table
def getdata(host,port,user,passwd,dbname,tb,cols):	
	mydb = mysql.connector.connect(
	  host=host,
	  user=user,
	  passwd=passwd,
	  database=dbname
	)
 
	mycursor = mydb.cursor()
	s = "SELECT "+cols[0]
	for i in range(1,len(cols)):
		s += ","
		s += cols[i]
	s += " FROM "+tb
	mycursor.execute(s)

	myresult = mycursor.fetchall()
	res = np.transpose(myresult)
	return res



######  Output Data of given Columns of a Table in DBS
def getdataindbs(host,port,user,passwd,dbname,tb,cols,dtypes):	
	mydb = mysql.connector.connect(
	  host=host,
	  user=user,
	  passwd=passwd,
	  database=dbname
	)
 
	mycursor = mydb.cursor()

	col1 = []
	col2 = []
	for i in range(0,len(cols)):
		if dtypes[i].upper().find("VARCHAR") != -1 or dtypes[i].upper().find("DATETIME") != -1:
			col2.append(cols[i])
		else:
			col1.append(cols[i])

	print(col1)
	print(col2)
	if len(col1) == 0:
		s = "SELECT "+col2[0]
		for i in range(1,len(col2)):
			s += ","
			s += col2[i]
		s += " FROM "+tb
	elif len(col2) == 0:
		s = "SELECT "+col1[0]
		for i in range(1,len(col1)):
			s += ","
			s += col1[i]
		s += " FROM "+tb
	else:
		s = "SELECT "+col2[0]
		for i in range(1,len(col2)):
			s += ","
			s += col2[i]
		for i in range(0,len(col1)):
			s += ", "
			s += "SUM("+col1[i]+")"
		s += " FROM "+tb
		s += " GROUP BY " + col2[0]
		for i in range(1,len(col2)):
			s += ","
			s += col2[i]
		print(s)
	mycursor.execute(s)

	myresult = mycursor.fetchall()
	res = np.transpose(myresult)
	return res,s





######  Output Column Names of a Table
def getcols(host,port,user,passwd,dbname,tb):
	mydb = mysql.connector.connect(
	  host=host,
	  user=user,
	  passwd=passwd,
	  database=dbname
	)

	mycursor = mydb.cursor()
	mycursor.execute("DESC "+tb)
	myresult = mycursor.fetchall()

	res = []
	colname = []
	datatype = []
	key = []
	for x in myresult:
		colname.append(x[0])
		datatype.append(x[1])
		key.append(x[3])
	res.append(colname)
	res.append(datatype)
	res.append(key)
	return res





###### Join Two Table and Output Column names of the Joined Table
def jointable(host,port,user,passwd,dbname,tables,headers,tbs,cols,order=0,group=0,limit=0,orderby='def',orderbyorder='def',groupby='def',lim='def'):
	mydb = mysql.connector.connect(
	  host=host,
	  user=user,
	  passwd=passwd,
	  database=dbname
	)

	tmp = []
	s = "CREATE OR REPLACE VIEW view1 as SELECT ";
	s += tables[0]+"."+headers[0]
	s += " as "+tables[0]+"_"+headers[0]
	tmp.append(tables[0]+"_"+headers[0])
	for i in range(1,len(tables)):
		s += ","+tables[i]+".";
		s += headers[i]
		s += " as "+tables[i]+"_";
		s += headers[i]
		tmp.append(tables[i]+"_"+headers[i])
	s += " FROM "
	s += tbs[0]
	for i in range(1,len(tbs)):
		s += ","+tbs[i]
	if len(tbs)>1:
		s += " WHERE "
		s += tbs[0]+"."+cols[0]
		s += "="
		s += tbs[1]+"."+cols[1]

		for i in range(2,len(tbs)):
			s += " AND "
			s += tbs[i-1]+"."+cols[i-1]
			s += "="
			s += tbs[i]+"."+cols[i]

	if group == '1':
		s += " GROUP BY "
		s += groupby
	if order == '1':
		s += " ORDER BY "
		s += orderby
		s += " "+ orderbyorder
	if limit == '1':
		s += " LIMIT "
		s += lim

	mycursor = mydb.cursor()
	mycursor.execute(s)
	res = getdata(host,port,user,passwd,dbname,'view1',tmp)
	return res

def execute(host,port,user,passwd,dbname, x, reportname):
	mydb = mysql.connector.connect(
		host=host,
		user=user,
		passwd=passwd,
		database=dbname
	)

	x = "CREATE OR REPLACE VIEW "+reportname+" as " + x
	mycursor = mydb.cursor()
	mycursor.execute(x)

	res1 = getcols(host,port,user,passwd,dbname, reportname)
	res = getdata(host, port, user, passwd, dbname, reportname, ['*'])
	return res


def opensavedreport(host, port, user, passwd, dbname, path):
	mydb = mysql.connector.connect(
		host=host,
		user=user,
		passwd=passwd,
		database=dbname
	)
	mycursor = mydb.cursor()
	mycursor.execute("DROP TABLE IF EXISTS csv;")

	df = pd.read_csv(path, header=0)
	data = excel.getcols(df)

	engine = create_engine('mysql://'+user+':'+passwd+'@'+host+':'+port+'/'+dbname)
	with engine.connect() as conn, conn.begin():
		df.to_sql('csv', conn, if_exists='append', index=False)

	x = "CREATE OR REPLACE VIEW view1 as SELECT * from csv;"
	mycursor.execute(x)

	return data

def uploadexcel(host, port, user, passwd, dbname, path, classname):
	mydb = mysql.connector.connect(
		host=host,
		user=user,
		passwd=passwd,
		database=dbname
	)
	mycursor = mydb.cursor()
	mycursor.execute("DROP TABLE IF EXISTS csv;")

	df = pd.read_csv(path, header=0)
	data = excel.getcols(df)

	engine = create_engine('mysql://' + user + ':' + passwd + '@' + host + ':' + port + '/' + dbname)
	with engine.connect() as conn, conn.begin():
		df.to_sql('csv', conn, if_exists='append', index=False)

	x = "CREATE OR REPLACE VIEW " + classname + " as SELECT * from csv;"
	mycursor.execute(x)

	return data

def runquery(host,port,user,passwd,dbname, x):
	mydb = mysql.connector.connect(
		host=host,
		user=user,
		passwd=passwd,
		database=dbname
	)
	mycursor = mydb.cursor()
	mycursor.execute(x)