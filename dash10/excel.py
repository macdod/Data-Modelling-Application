import pandas as pd
import numpy as np 



######  Output Data of given Columns of a Table
def getdata(tb,cols):
	data = []
	for i in cols:
		l = [x for x in tb[i]]
		y = np.transpose(l)
		data.append(y)
	return data




######  Output Column Names of a Table
def getcols(tb):
	df = tb.columns
	l = []
	for x in tb.columns:
		l.append(x)
	return l



###### Join Two Table and Output Column names of the Joined Table
def jointable(tb1,tb2,col1,col2):
	l = [x for x in tb1.columns]
	r = [x for x in tb2.columns]
	cols = set()
	for i in l:
		cols.add(i)
	for i in r:
		cols.add(i)
	res = []
	for i in cols:
		res.append(i)
	return res