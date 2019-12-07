  
###############    IMPORTS    ####################

###   DJANGO IMPORTS
from django.shortcuts import render,redirect
from django.http import HttpResponse , JsonResponse, HttpResponseRedirect
from django.core import serializers
from django.http import Http404
from django.db import connection
 
import json 
import numpy as np
import pandas as pd
import datetime as dt
import decimal as dc
import os
from . import plots
from . import import_data_model
from . import import_data
from .models import user, userdbs, userreports, userdatamodels, userdashboards


###   DATABASES FILE IMPORTS
from . import mysqldb
from . import oracledb
from . import excel



####################    MAIN VIEWS      ###########################

###### INDEX VIEW
def index(Request) :
	if 'useremail' in Request.session:
		context = {'username' : Request.session['username'], 'useremail' : Request.session['useremail']}
		return render(Request, "dash10/home.html", context)
	return render(Request, "dash10/index.html")



###### SIGNUP VIEW
def signup(Request) :
	return render(Request, "dash10/signup.html")




###### SIGNUP HELPER VIEW
def signupcreate(Request) :
	if Request.method == 'POST':

		###   VARIABLES
		username = Request.POST['user']
		useremail = Request.POST['email']
		userpasswd = Request.POST['passwd']


		###   EMAIL ALREADY EXISTS
		if user.objects.filter(email = useremail).exists():
			context = {'message' : 'Email Already Exists'}
			return render(Request, "dash10/signup.html", context)

		###   CREATE NEW USER
		newuser = user(user = str(username),email = str(useremail),passwd = str(userpasswd))
		newuser.save()
		
		os.chdir('dash10/static/uploads')
		os.mkdir(username)
		os.chdir(username)
		os.mkdir('datamodel')
		os.mkdir('report')
		os.mkdir('dashboard')
		os.chdir('../../../../')


		###   STORE USERNAME AND USEREMAIL IN SESSION
		Request.session['username'] = username
		Request.session['useremail'] = useremail

		###   RETURN THE VIEW
		context = {'username' : username, 'useremail' : useremail}
		return render(Request, "dash10/home.html",context)
	else:
		return render(Request, "dash10/index.html")


###### SIGNIN VIEW
def signin(Request) :
	return render(Request, "dash10/signin.html")




###### SIGNIN HELPER VIEW
def signinuser(Request) :
	if Request.method == 'POST':

		###   VARIABLES
		useremail = Request.POST['email']
		userpasswd = Request.POST['passwd']


		###   USER HAVE A ACCOUNT ?
		if user.objects.filter(email = useremail).exists() and user.objects.get(email = useremail).passwd == userpasswd:


			###   STORE USERNAME AND USEREMAIL IN SESSION
			Request.session['username'] = user.objects.get(email = useremail).user
			Request.session['useremail'] = useremail


			###   RETURN THE VIEW
			context = {'username' : Request.session['username'], 'useremail' : useremail}
			return render(Request, "dash10/home.html",context)

		### USER DOESN'T HAVE A ACCOUNT
		return render(Request, "dash10/signin.html")
	else:
		return render(Request, "dash10/index.html")



###### LOGOUT VIEW
def logout(Request) :

	###  DELETING SESSION
	if 'useremail' in Request.session:
		del Request.session['useremail']
	if 'username' in Request.session:
		del Request.session['username']
	if 'userdb' in Request.session:
		del Request.session['userdb']

	###   RETURN THE VIEW
		return render(Request, "dash10/index.html")
	else:
		return render(Request, "dash10/index.html")

####################    ////////////  MAIN  VIEWS      ###########################







####################   OPTIONS TO CREATE DASHBOARD VIEWS  ##############



##### RENDER MYDATAMODELS PAGE
def mydashboards(Request):
	a = user.objects.get(email = Request.session['useremail'])
	c = userdashboards.objects.filter(email=a)
	context = {'username' : a.user , 'dashboards': c, 'useremail' : Request.session['useremail']}
	return render(Request, "dash10/mydashboards.html", context)



def createdashboard(Request):
	if 'useremail' in Request.session:
		useremail = Request.session['useremail']
		a = user.objects.get(email = useremail)
		context = {'username' : a.user, 'useremail': useremail}
		return render(Request,"dash10/options.html", context)


def savedashboard(Request):
	if 'useremail' in Request.session:
		useremail = Request.session['useremail']
		a = user.objects.get(email = useremail)
		data = Request.POST['data']
		filename = Request.POST['filename']
		data = json.loads(data)
		print(data)
		print(type(data))
		c = userdashboards(email = a, dashboardname = filename)
		c.save()
		path = './dash10/static/uploads/'+a.user+'/dashboard/'+filename+'.csv'

		df = pd.DataFrame.from_dict(data)
		df.to_csv(path, index=False)

		return HttpResponse("success")


def importdashboard(Request,dashboard):
	if Request.method == 'POST' and 'useremail' in Request.session:
		useremail = Request.session['useremail']
		a = user.objects.get(email = useremail)
		path = Request.POST['path']
		print(path)
		df_new = pd.read_csv(path)
		dict_new = {}

		for i in df_new.columns:
			dict_new[int(i)]= list(df_new[i])

		print(dict_new)


		return HttpResponse(json.dumps(dict_new))
	else:
		a = user.objects.get(email = Request.session['useremail'])
		useremail = Request.session['useremail']
		dbtype = Request.session['userdb']
		dbname = Request.session['dbname']
		a = user.objects.get(email = useremail)
		b = userdbs.objects.get(email = a, dbtype = dbtype, dbname = dbname)
		tables = mysqldb.getviewnameswithrep(b.host, b.port, b.username, b.passwd, dbname)
		return render(Request, "dash10/dbs.html", context = {'loaddb' : 2, 'filename': dashboard, 'username' : a.user, 'tables' : tables})





###################  ////// OPTIONS TO CREATE DASHBOARD VIEWS ############











#################    DATA SOURCE CONNECTION VIEWS  ###########

def dbconn(Request) :
	if Request.method == 'POST':
		dbtype = Request.POST['dbtype']
		host = Request.POST['host']
		port = Request.POST['port']
		username = Request.POST['user']
		password = Request.POST['password']
		dbname = Request.POST['dbname']
		useremail = Request.session['useremail']

		###   STORE DB TYPE IN SESSION
		Request.session['userdb'] = dbtype
		if dbtype == 'excel':
			Request.session['file'] = Request.FILES['uploadfile'].name

		a = user.objects.get(email=useremail)
		if userdbs.objects.filter(email=a, dbtype=dbtype, host=host, port=port, username=username, passwd=password, dbname=dbname).exists() == False:
			b = userdbs(email=a, dbtype=dbtype, host=host, port=port, username=username, passwd=password, dbname=dbname)
			if dbtype == 'mysql':
					b.save()
			elif dbtype == 'oracle':
				b.save()



	useremail = Request.session['useremail']
	b = userdbs.objects.filter(email__email = useremail)
	context = {'b': b, 'username': Request.session['username']}
	return render(Request, "dash10/dbconn.html", context)




###### Data Source Add
def dbconnadd(Request) :
	if Request.method == 'POST':

		###VARIABLES
		Request.session['userdb'] = Request.POST['dbtype']
		Request.session['dbname'] = Request.POST['dbname']
		print("abcd")

	return HttpResponse("hello")

#################    //////DATA SOURCE CONNECTION VIEWS  ###########








##################    DATA MODELING VIEWS  #################

#### CHANGE GETCOLUMNS METHOD TO RETURN COLUMN NAMES WITH DATA TYPES AND KEYS



def loaddatamodel(Request):
	if Request.method == 'POST':
		datamodelname = Request.POST['datamodelname']
		Request.session['datamodelname'] = datamodelname

		return redirect("/dash10/datamodel/")


####     DATA MODEL
def datamodel(Request):
	if Request.method == 'POST':

		useremail = Request.session['useremail']
		a = user.objects.get(email = useremail)

		dbtype = Request.POST['dbtype']
		host = Request.POST['host']
		port = Request.POST['port']
		username = Request.POST['user']
		password = Request.POST['password']
		dbname = Request.POST['dbname']


		Request.session['userdb'] = dbtype
		Request.session['dbname'] = dbname

		if userdbs.objects.filter(email = a, dbtype = dbtype, dbname = dbname).exists():
			b = userdbs.objects.get(email = a, dbtype = dbtype, dbname = dbname)
			b.host = host
			b.port = port
			b.username = username
			b.passwd = password
			b.dbname = dbname
			b.save()
		else:
			b = userdbs(email = a, dbtype = dbtype, host = host, port = port, username = username, passwd = password, dbname = dbname)
			b.save()



		tables = []
		if dbtype == 'mysql':
			tables = mysqldb.gettablenames(host,port,username,password,dbname)
		elif dbtype == 'oracle':
			tables = oracledb.gettablenames(host,port,username,password,dbname)

		context = {'username' : a.user, 'useremail' : useremail, 'dbname':dbname, 'tables': tables}
		return render(Request, "dash10/datamodel.html", context)
	elif Request.method == 'GET':
		if 'userdb' in Request.session:
			useremail = Request.session['useremail']
			dbtype = Request.session['userdb']
			dbname = Request.session['dbname']
			a = user.objects.get(email = useremail)
			b = userdbs.objects.get(email = a, dbtype = dbtype, dbname = dbname)
			tables = []
			if dbtype == 'mysql':
				tables = mysqldb.gettablenames(b.host,b.port,b.username,b.passwd,dbname)
			elif dbtype == 'oracle':
				tables = oracledb.gettablenames(b.host,b.port,b.username,b.passwd,b.dbname)

			context = {'username' : a.user, 'useremail' : useremail, 'dbname':dbname, 'tables': tables}
			return render(Request, "dash10/datamodel.html", context)



def savedatamodel(Request):
	if Request.method == 'POST':
		classes = Request.POST.getlist('classes[]')
		objects = Request.POST.getlist('objects[]')
		types = Request.POST.getlist('types[]')
		keys = Request.POST.getlist('keys[]')
		jointables = Request.POST.getlist('jointables[]')
		joincolumns = Request.POST.getlist('joincolumns[]')
		subjectarea = Request.POST.getlist('subjectarea[]')
		filename = Request.POST['filename']
		useremail = Request.session['useremail']
		a = user.objects.get(email = useremail)
		username = a.user
		dbtype = Request.session['userdb']
		dbname = Request.session['dbname']
		b = userdbs.objects.get(email = a, dbtype = dbtype, dbname = dbname)
		c = userdatamodels(email=a, datamodelname=filename, databasetype = dbtype,databasename = dbname )
		c.save()

		Request.session['filename'] = filename


		print(classes)
		print(objects)
		print(types)
		print(keys)
		print(jointables)
		print(joincolumns)
		print(filename)
		print(subjectarea)

		import_data_model.Export_Data_Model(subjectarea[0], classes, objects, types, keys, jointables, joincolumns, 'dash10/static/uploads/'+username+'/datamodel', filename, filename + 'join')

		return HttpResponse("Success")


##### RENDER MYDATAMODELS PAGE
def mydatamodels(Request):
	a = user.objects.get(email = Request.session['useremail'])
	c = userdatamodels.objects.filter(email=a)
	context = {'username' : a.user , 'datamodels': c, 'useremail' : Request.session['useremail']}
	return render(Request, "dash10/mydatamodels.html", context)




def importdatamodel(Request,datamodel):
	if Request.method == 'POST':
		filepath = Request.POST['filepath']
		joinpath = Request.POST['joinpath']
		print(filepath)
		print(joinpath)
		print("path")
		subjectarea,tables1,columns,types,keys,tables,jointables,joincolumns = import_data_model.Import_data_model_Hybrid(filepath, joinpath)
		print(subjectarea)
		print(tables)
		print(columns)
		print(types)

		for i in range(len(keys)):
			if keys[i] == 'Nan':
				keys[i]=''

		print(jointables)
		print(joincolumns)
		print(tables1)
		print(keys)

		res = {}
		res[0] = subjectarea
		res[1] = tables
		res[2] = tables1
		res[3] = columns
		res[4] = types
		res[6] = jointables
		res[7] = joincolumns

		return HttpResponse(json.dumps(res))
	else:
		a = user.objects.get(email = Request.session['useremail'])
		c = userdatamodels.objects.get(email = a, datamodelname = datamodel)
		useremail = Request.session['useremail']
		dbtype =  c.databasetype
		dbname = c.databasename
		Request.session['userdb'] = dbtype
		Request.session['dbname'] = dbname
		print("gufgwgfuhgrwghroigrh---------------------------------------------------------------------------")
		print(dbtype)
		a = user.objects.get(email = useremail)
		b = userdbs.objects.get(email = a, dbtype = dbtype, dbname = dbname)
		tables = []
		if dbtype == 'mysql':
			tables = mysqldb.gettablenames(b.host,b.port,b.username,b.passwd,dbname)
		elif dbtype == 'oracle':
			tables = oracledb.gettablenames(b.host,b.port,b.username,b.passwd,b.dbname)
		return render(Request, "dash10/datamodel.html", context = {'load' : 1, 'filename': datamodel, 'username' : a.user, 'tables': tables})


##################    ///////DATA MODELING VIEWS  #################









#########################   REPORT VIEWS    ##########################

######  RENDER THE QUERY FILTER PAGE
def query(Request) :
	if Request.method == 'POST':

		if Request.POST['whichmethod'] == '0':
			classname = Request.POST['classname']
			Request.session['file'] = Request.FILES['uploadfile'].name
			reportname = Request.POST['reportname']
			reportname = 'rep$' + reportname

			file = Request.FILES['uploadfile']
			handle_uploaded_file(file)
			path = 'dash10/static/uploads/' + file.name
			print("JHLLGLGLGLGL")
			sample = mysqldb.uploadexcel('localhost', '3306', 'root', '', 'dashdesk', path, classname)

			context = {'username': Request.session['username'], 'tables': [classname], 'reportname':reportname}
			return render(Request, "dash10/query.html", context)

		elif Request.POST['whichmethod'] == '1':
			useremail = Request.session['useremail']
			a = user.objects.get(email=useremail)

			dbtype = Request.POST['dbtype']
			host = Request.POST['host']
			port = Request.POST['port']
			username = Request.POST['user']
			password = Request.POST['password']
			dbname = Request.POST['dbname']
			classname = Request.POST['classname']
			sqlquery = Request.POST['sqlquery']
			reportname = Request.POST['reportname']
			reportname = 'rep$' + reportname

			Request.session['userdb'] = dbtype
			Request.session['dbname'] = dbname

			if userdbs.objects.filter(email=a, dbtype=dbtype, dbname=dbname).exists():
				b = userdbs.objects.get(email=a, dbtype=dbtype, dbname=dbname)
				b.host = host
				b.port = port
				b.username = username
				b.passwd = password
				b.dbname = dbname
				b.save()
			else:
				b = userdbs(email=a, dbtype=dbtype, host=host, port=port, username=username, passwd=password, dbname=dbname)
				b.save()

			print("123456789")
			tables = []
			if dbtype == 'mysql':
				sqlquery = "CREATE OR REPLACE VIEW "+classname+" as "+sqlquery
				mysqldb.runquery(host, port, username, password, dbname, sqlquery)
				# tables = mysqldb.getcols(host, port, username, password, dbname, 'view2')
			elif dbtype == 'oracle':
				tables = oracledb.gettablenames(host, port, username, password, dbname)

			context = {'username': Request.session['username'], 'tables': [classname], 'reportname':reportname}
			return render(Request, "dash10/query.html", context)


	else:
		reportname = Request.session['reportname']
		reportname = 'rep$' + reportname

		print("UUUUUUUUU")
		useremail = Request.session['useremail']
		dbtype = Request.session['userdb']
		dbname = Request.session['dbname']

		a = user.objects.get(email=useremail)
		b = userdbs.objects.get(email=a, dbtype=dbtype, dbname=dbname)

		Request.session['userdb'] = 'mysql'
		Request.session['dbname'] = 'dashdesk'
		tables = mysqldb.getviewnameswithoutrep('localhost', 3306, 'root', '', 'dashdesk')
		context = {'username': Request.session['username'], 'tables': tables, 'reportname': reportname}
		return render(Request, "dash10/query.html", context)


##### RENDER MYREPORTS PAGE
def myreports(Request):
	a = user.objects.get(email = Request.session['useremail'])
	c = userreports.objects.filter(email=a)
	context = {'username' : a.user , 'reports': c, 'useremail' : Request.session['useremail']}
	return render(Request, "dash10/myreports.html", context)


#####  OPEN SAVED REPORTS
def opensavedreport(Request) :
	if Request.method == 'POST':
		filename = Request.POST['filename']
		username = Request.POST['username']
		useremail = Request.session['useremail']

		path = 'dash10/static/uploads/' + username + '/report/' + filename
		df = pd.read_csv(path, header=0)
		data = excel.getcols(df)


		headers = mysqldb.opensavedreport('localhost', '3306', 'root', '', 'dashdesk', path)
		tables = ['view1']


		for i in range(1, len(headers)):
			tables.append('view1')

		Request.session['tables'] = tables
		Request.session['headers'] = headers
		Request.session['sql'] = 1
		print(headers)

		return HttpResponse("hello")


###### RENDER THE TABLE.HTML
def report(Request):
	useremail = Request.session['useremail']
	a = user.objects.get(email=useremail)

	username = a.user 
	dbtype = Request.session['userdb']
	dbname = Request.session['dbname']
	b = userdbs.objects.get(email=a, dbtype=dbtype, dbname=dbname)
	filename = Request.session['filename']
	path = './dash10/static/uploads/' + username + '/datamodel/'
	print("importing")
	subjectarea = import_data.Import_data_model(path + filename + '.csv', path + filename + 'join.csv', b.host, b.port,
												b.username, b.passwd, dbtype, dbname)
	print("imported")
	Request.session['views'] = subjectarea

	Request.session['reportname'] = Request.POST['reportname']
	return redirect("/dash10/query/")


######  TABLE
def table(Request):
	if Request.method == 'POST':


		###    VARIABLES
		tables = Request.POST.getlist('tables[]')
		headers = Request.POST.getlist('headers[]')
		tbs = Request.POST.getlist('tbs[]')
		cols = Request.POST.getlist('cols[]')
		order = Request.POST['order']
		group = Request.POST['group']
		limit = Request.POST['limit']
		orderby = Request.POST['orderby']
		groupby = Request.POST['groupby']
		lim = Request.POST['lim']
		orderbyorder = Request.POST['orderbyorder']
		useremail = Request.session['useremail']
		userdb = Request.session['userdb']
		Request.session['sql'] = 0

		###    DATABASES
		a = user.objects.get(email = useremail)
		b = userdbs.objects.get(email = a,dbtype = userdb)



		###  STORE DATA IN SESSION
		Request.session['tables'] = tables
		Request.session['headers'] = headers


		if userdb == 'mysql':
			ret = mysqldb.jointable(b.host,b.port,b.username,b.passwd,b.dbname,tables,headers,tbs,cols,order,group,limit,orderby,orderbyorder,groupby,lim)
		elif userdb == 'oracle':
			ret = oracledb.jointable(b.host,b.port,b.username,b.passwd,b.dbname,tables,headers,tbs,cols,order,group,limit,orderby,orderbyorder,groupby,lim)


		###  TYPE CONVERSION FOR INT32, DATETIME, DECIMAL
		res = {}
		for i in range(0,len(headers)):
			if type(ret[i][0]) == np.int32:
				res[i] = list([np.float64(j) for j in ret[i]])
			elif isinstance(ret[i][0], dt.datetime):
				res[i] = list([str(j) for j in ret[i]])
			elif type(ret[i][0]) == dc.Decimal:
				res[i] = list([np.float64(j) for j in ret[i]])
			else:
				res[i] = list(ret[i])


		# # ###    RETURN THE DICTS OF (LIST OF VALUES FOR ALL COLUMNS)
		return HttpResponse(json.dumps(res))




###  CREATE TABLE THROUGH CUSTOM SQL
def customsql(Request):
	if Request.method == 'POST':

		x = Request.POST['x']
		dbname = Request.session['dbname']
		dbtype = Request.session['userdb']
		useremail = Request.session['useremail']
		reportname =Request.POST['reportname']
		Request.session['sql'] = 1
		a = user.objects.get(email=useremail)
		b=userdbs.objects.get(email = a, dbname = dbname, dbtype = dbtype)

		if dbtype == 'mysql':
			ret = mysqldb.execute(b.host, b.port, b.username, b.passwd, dbname, x, reportname)
			res1 = mysqldb.getcols(b.host, b.port, b.username, b.passwd, dbname, reportname)

			tables = [reportname]
			for i in range(1, len(res1[0])):
				tables.append(reportname)

			Request.session['tables'] = tables
			Request.session['headers'] = res1[0]
			res={}

			res[0] = res1[0]

			for i in range(0, len(ret)):
				if type(ret[i][0]) == np.int32:
					res[i+1] = list([np.float64(j) for j in ret[i]])
				elif isinstance(ret[i][0], dt.datetime):
					res[i+1] = list([str(j) for j in ret[i]])
				elif type(ret[i][0]) == dc.Decimal:
					res[i+1] = list([np.float64(j) for j in ret[i]])
				else:
					res[i+1] = list(ret[i])

			print(res)
			return HttpResponse(json.dumps(res))



######  SAVE REPORT IN CSV FORMAT
def savereport(Request):
	if Request.method == 'POST':

		filename = Request.POST['filename']
		useremail = Request.session['useremail']
		userdb = Request.session['userdb']

		###    DATABASES
		a = user.objects.get(email = useremail)
		b = userdbs.objects.get(email = a, dbtype = userdb)
		c = userreports(email=a, reportname=filename )
		c.save()

		if userdb == 'mysql':
			columns = mysqldb.getcols(b.host,b.port,b.username,b.passwd,b.dbname,'view1')
			data = mysqldb.getdata(b.host,b.port,b.username,b.passwd,b.dbname,'view1',columns[0])
		elif userdb =='oracle':
			columns = oracledb.getcols(b.host,b.port,b.username,b.passwd,b.dbname,'view1')
			data = oracledb.getdata(b.host,b.port,b.username,b.passwd,b.dbname,'view1',columns)

		print(columns)
		path = os.getcwd()
		path = path + '/dash10/static/uploads/'+a.user+'/report/'+filename
		tocsv(columns[0], data, path)
		###    RETURN LIST OF COLUMN NAMES OF JOINED TABLES
		return HttpResponse(json.dumps(columns[0]))

#################################  ////////// REPORT VIEWS    ##############################











##########################  DASHBOARD VIEWS  ######################

###### DATABASE SETUP HANDLER VIEW
def dbs(Request) :
	if Request.method == 'POST':
		reportname = Request.POST['reportname']
		reportname = 'rep$'+reportname
		q = "CREATE OR REPLACE VIEW "+reportname+" as SELECT * FROM view1;"
		print(q)
		mysqldb.runquery('localhost', 3306, 'root', '', 'dashdesk', q)

	# 	###   VARIABLES
	# 	dbtype = Request.POST['dbtype']
	# 	host = Request.POST['host']
	# 	port = Request.POST['port']
	# 	username = Request.POST['user']
	# 	password = Request.POST['password']
	# 	dbname = Request.POST['dbname']
	# 	useremail = Request.session['useremail']


	# 	###   STORE DB TYPE IN SESSION
	# 	Request.session['userdb'] = dbtype
	# 	if dbtype == 'excel':
	# 		Request.session['file'] = Request.FILES['uploadfile'].name
		

	# 	###    CHECK FOR DB EXISTS FOR USER
	# 	a = user.objects.get(email = useremail)
	# 	if userdbs.objects.filter(email = a, dbtype = dbtype).exists():
	# 		b = userdbs.objects.get(email = a, dbtype = dbtype)
	# 		b.host = host
	# 		b.port = port
	# 		b.username = username
	# 		b.passwd = password
	# 		b.dbname = dbname
	# 		b.save()
	# 	else:
	# 		b = userdbs(email = a, dbtype = dbtype, host = host, port = port, username = username, passwd = password, dbname = dbname)
	# 		b.save()


	# 	###    DATABASES
	# 	if dbtype == 'mysql':
	# 		data = mysqldb.gettablenames(host,port,username,password,dbname)
	# 	elif dbtype == 'oracle':
	# 		data = oracledb.gettablenames(host,port,username,password,dbname)
	# 	elif dbtype == 'excel':
	# 		file = Request.FILES['uploadfile']
	# 		handle_uploaded_file(file)
	# 		path = 'dash10/static/uploads/'+file.name
	# 		df = pd.read_csv(path)
	# 		data = excel.getcols(df)


	# 		####   RETURN THE VIEW
	# 		context = {'data' : data, 'username' : Request.session['username']}
	# 		return render(Request, "dash10/dbmodels.html", context)


	# 	####   RETURN THE VIEW
	# 	context = {'data' : mylist ,'username' : Request.session['username'], 'sql' : Request.session['sql']}
	# 	return render(Request, "dash10/dbs.html", context)
	# else:

		###    USER ALREADY SET UP A DB
	if 'userdb' in Request.session:
		a = user.objects.get(email = Request.session['useremail'])
		db = userdbs.objects.get(email = a, dbtype = Request.session['userdb'])
		# if db.dbtype == 'mysql':
		# 	data = mysqldb.gettablenames(db.host,db.port,db.username,db.passwd,db.dbname)
		# elif db.dbtype == 'oracle':
		# 	data = oracledb.gettablenames(db.host,db.port,db.username,db.passwd,db.dbname)
		# elif db.dbtype == 'excel':
		# 	path = 'dash10/static/uploads/'+Request.session['file']
		# 	df = pd.read_csv(path)
		# 	data = excel.getcols(df)

			# context = {'data' : data, 'username' : a.user}
			# return render(Request, "dash10/dbmodels.html", context)

		###   
		tables = mysqldb.getviewnameswithrep('localhost',3306,'root','', 'dashdesk')
		print(tables)


		context = {'tables' : tables , 'username' : a.user}
		return render(Request, "dash10/dbs.html", context)


	###   USER HAS A SESSION
	elif 'useremail' in Request.session:
		context = {'username' : Request.session['username'], 'useremail': Request.session['useremail']}
		return render(Request, "dash10/dbconn.html", context)
	else:
		return render(Request, "dash10/index.html")




###     DATABASE HELPER VIEW
def dbhelper(Request):
	if Request.method == 'GET' and 'useremail' in Request.session:

		###   VARIABLES
		dbtype = Request.GET['dbtype']
		useremail  = Request.session['useremail']


		###   DBTYPE EXISTS FRO CURRENT USER ?
		a = user.objects.get(email = useremail)
		if userdbs.objects.filter(email = a, dbtype = dbtype).exists():
			Request.session['userdb'] = dbtype
			return HttpResponseRedirect('/dash10/dbs/')
		else:
			context = {username : Request.session['username'], useremail: Request.session['useremail']}
			return render(Request, "dash10/dbsetup.html", context)



###### PLOT PAGE
def plotpage(Request,selectedplot):
	print("------- >>>>")
	print(selectedplot)
	return render(Request, "dash10/plots/plot"+selectedplot+".html")


######  PLOT THE FIGURE
def plot(Request):
	if Request.method == 'POST':


		###   VARIABLES
		c1 = Request.POST['c1']
		c2 = Request.POST['c2']
		c3 = Request.POST['c3']
		dt1 = Request.POST['dt1']
		dt2 = Request.POST['dt2']
		dt3 = Request.POST['dt3']
		selectedplot = Request.POST['selectedplot']
		Request.session['selectedplot'] = selectedplot
		print("selectedplot")
		print(selectedplot)
		print(dt1)
		print(dt2)
		print(dt3)

		###  CHECK FOR THE THIRD DIMENSION
		if c3 == "def":
			dtypes = [dt1,dt2]
			data = [c1,c2] 
		else:
			dtypes = [dt1,dt2,dt3]
			data = [c1,c2,c3]

		tb = Request.POST['tb']
		met = Request.POST['mets']
		color = Request.POST['color']
		useremail = Request.session['useremail']
		userdb  = Request.session['userdb']

		print(tb)
		print(data)

		###    DATABASES
		a = user.objects.get(email = useremail)
		b = userdbs.objects.get(email = a, dbtype = userdb)
		cols,sqlquery = mysqldb.getdataindbs('localhost', 3306, 'root', '', 'dashdesk', tb, data, dtypes)
		# if userdb == 'mysql':
		# 	cols = mysqldb.getdata(b.host,b.port,b.username,b.passwd,b.dbname,tb,data)
		# elif userdb == 'oracle':
		# 	cols = oracledb.getdata(b.host,b.port,b.username,b.passwd,b.dbname,tb,data)
		# elif userdb == 'excel':
		# 	path = 'dash10/static/uploads/'+Request.session['file']
		# 	df = pd.read_csv(path)
		# 	cols = excel.getdata(df,data)


		###    PLOT METHODS
		if c3 == "def":
			if dt1.upper().find("VARCHAR") == -1  and dt1.upper().find("DATETIME") == -1:
				c1 = 'SUM OF '+c1
			if dt2.upper().find("VARCHAR") == -1  and dt2.upper().find("DATETIME") == -1:
				c2 = 'SUM OF '+c2
		else:
			if dt1.upper().find("VARCHAR") == -1  and dt1.upper().find("DATETIME") == -1:
				c1 = 'SUM OF '+c1
			if dt2.upper().find("VARCHAR") == -1  and dt2.upper().find("DATETIME") == -1:
				c2 = 'SUM OF '+c2
			if dt3.upper().find("VARCHAR") == -1  and dt3.upper().find("DATETIME") == -1:
				c3 = 'SUM OF '+c3
		if(met == "bar"):
			if len(data)<=2:
				plots.Bar_plot(cols[0],cols[1],color,c1,c2,selectedplot)
			else:
				plots.Bar_plot(cols[0],cols[1],color,c1,c2,selectedplot,cols[2])
		elif(met == "pie"):
			plots.Pie_chart(cols[0],cols[1],selectedplot)
		elif(met == "line"):
			if len(data)<=2:
				plots.Line_plot(cols[0],cols[1],color,c1,c2,selectedplot)
			else:
				plots.Line_3d(cols[0],cols[1],cols[2],c1,c2,c3,selectedplot)
		else:
			if len(data)<=2:
				plots.Scatter_plot(cols[0],cols[1],color,c1,c2,selectedplot)
			else:
				plots.Scatter_3d(cols[0],cols[1],cols[2],c1,c2,c3,selectedplot)
		result = []
		result.append(sqlquery)
		result.append(selectedplot)
		result.append(met)
		return HttpResponse(json.dumps(result))



######  SAVE REPORT
def applyfilters(Request):
	if Request.method == 'POST':


		###    VARIABLES
		cols = Request.POST.getlist('data[]')
		filter_type = Request.POST.getlist('filter_type[]')
		filter_val = Request.POST.getlist('filter_val[]')
		fig_type = Request.POST['fig_type']
		color = Request.POST['color']
		print("cllmm")
		print(cols)
		print(filter_type)
		print(filter_val)
		print(fig_type)

		useremail = Request.session['useremail']
		userdb = Request.session['userdb']


		###    DATABASES
		a = user.objects.get(email = useremail)
		b = userdbs.objects.get(email = a, dbtype = userdb)


		if userdb == 'mysql':
			data = mysqldb.getdata(b.host,b.port,b.username,b.passwd,b.dbname,'view1',cols)
		elif userdb =='oracle':
			data = oracledb.jointable(b.host,b.port,b.username,b.passwd,b.dbname,'view1',cols)

		print("djndjknf")
		print(data)
		plots.apply(data[0],data[1],fig_type,filter_type,filter_val,color)
		return HttpResponse(json.dumps(cols)) 

############################  /////////////DASHBOARD VIEWS    ###########################









#################   OTHER FUNCTION  ###############


#####    FILE UPLOAD

def handle_uploaded_file(f):
	with open('dash10/static/uploads/'+f.name, 'wb+') as destination:
		for chunk in f.chunks():
			destination.write(chunk)



def tocsv(headers,cells,filename):
	cells = np.array(cells)
	cells = np.transpose(cells)
	df = pd.DataFrame(cells,columns = headers)
	df.to_csv(filename, index=False)


def group_columns(tables,headers):
	tb=pd.Series(np.array(tables))
	col=pd.Series(np.array(headers))
	print(tb)
	print(col)
	df=pd.concat([tb,col], axis=1)
	df=df.groupby([0])[1].apply(list).reset_index()
	cols=[]
	tabs=[]
	for l in df.iloc[:,1]:
		cols.append(l)
	for t in df.iloc[:,0]:
		tabs.append(t)
	return tabs,cols



######  Output Column Names
def getcolumns(Request) :
	if Request.method == 'POST':


		###   VARIABLES
		tb = Request.POST['tb']
		useremail = Request.session['useremail']
		userdb = Request.session['userdb']

		print("abcd --- >")
		print(tb)
		###    DATABASES
		a = user.objects.get(email = useremail)
		b = userdbs.objects.get(email = a, dbtype = userdb)
		if userdb == 'mysql':
			qs = mysqldb.getcols(b.host,b.port,b.username,b.passwd,b.dbname,tb)
		elif userdb == 'oracle':
			qs = oracledb.getcols(b.host,b.port,b.username,b.passwd,b.dbname,tb)

		res = {}
		res[0] = qs[0]
		res[1] = qs[1]
		res[2] = qs[2]

		###  RETURN LIST OF COLUMN NAMES OF TABLE (tb) 
		return HttpResponse(json.dumps(res))

#################   ////////////////OTHER FUNCTION  ###############



