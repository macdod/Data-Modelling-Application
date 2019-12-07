from django.db import models

class user(models.Model) :
	user = models.CharField(max_length=250)
	email = models.CharField(max_length=250)
	passwd = models.CharField(max_length=250)
	def __str__(self) :
		return self.user + ' --- ' + self.email

class userdbs(models.Model) :
	email = models.ForeignKey(user, on_delete = models.CASCADE)
	dbtype = models.CharField(max_length=250)
	host = models.CharField(max_length=250)
	port = models.CharField(max_length=250)
	username = models.CharField(max_length=250)
	passwd = models.CharField(max_length=250)
	dbname = models.CharField(max_length=250)
	def __str__(self) :
		return self.host + ' --- ' + self.dbtype

class userreports(models.Model) :
	email = models.ForeignKey(user, on_delete=models.CASCADE)
	reportname = models.CharField(max_length=250)
	def __str__(self):
		return self.reportname

class userdatamodels(models.Model) :
	email = models.ForeignKey(user, on_delete=models.CASCADE)
	datamodelname = models.CharField(max_length=250)
	databasetype =  models.CharField(max_length=250)
	databasename = models.CharField(max_length=250)
	def __str__(self):
		return self.datamodelname


class userdashboards(models.Model) :
	email = models.ForeignKey(user, on_delete=models.CASCADE)
	dashboardname = models.CharField(max_length=250)
	def __str__(self):
		return self.dashboardname