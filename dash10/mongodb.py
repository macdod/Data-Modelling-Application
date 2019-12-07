import pymongo
import numpy as np


 

######  Output Table Names
def gettablenames(host,port,user,passwd,dbname):
	myclient = pymongo.MongoClient("mongodb://localhost:27017/")
	mydb = myclient["test"]
	return mydb.list_collection_names()





######  Output Data of given Columns of a Table
def getdata(host,port,user,passwd,dbname,tb,cols):	





######  Output Column Names of a Table
def getcols(host,port,user,passwd,dbname,tb):






###### Join Two Table and Output Column names of the Joined Table
def jointable(host,port,user,passwd,dbname,tb1,tb2,col1,col2):
