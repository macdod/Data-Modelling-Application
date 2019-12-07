from django.shortcuts import render
from django.http import HttpResponse , JsonResponse
from django.core import serializers
from django.http import Http404
from django.db import connection

import json 
import numpy as np
 
def index(Request) :
	return render(Request, "temp/index.html")
