#!/usr/bin/python3

import findspark
findspark.init()
import time
from pyspark import SparkConf,SparkContext
from pyspark.sql import  Row,SQLContext,SparkSession
from pyspark.sql.functions import col
import pandas as pd
import sys
import json
import requests
import datetime
import csv
import calendar
from dateutil.relativedelta import relativedelta
from pyspark.ml.regression import LinearRegression 
from pyspark.ml.linalg import Vectors 
from pyspark.ml.feature import VectorAssembler 

conf=SparkConf()
conf.setAppName("FPL")
sc=SparkContext(master="local[4]",conf=conf)
sqlcontext=SQLContext(sc)
#id_list=[]
path = "players_rating.csv"
df = sqlcontext.read.csv(path, header=True)

df2 = sqlcontext.read.csv("players.csv",header=True).select("name","Id","birthDate")
df1 = sqlcontext.read.csv("players.csv",header=True).select("Id","birthDate")
dff=df.join(df1,on=['Id'],how='left')
dff.dropDuplicates()
#dff.show()
r = dff.collect()
l =[]
print(dff.count())
for i in range(dff.count()):
	date1 = datetime.datetime.strptime(r[i][2], "%Y-%m-%d").strftime("%Y,%m,%d")
	date2 = datetime.datetime.strptime(r[i][3], "%Y-%m-%d").strftime("%Y,%m,%d")
	d1=date1.split(',')
	d2=date2.split(',')
	age=int(d1[0])-int(d2[0])
	l.append((r[i][0],age))
	
#print(l)
ag=sqlcontext.createDataFrame(l,["Id","age"])
#ag.show()
dfff=dff.join(ag,on=['Id'],how='left')
fin=dfff.collect()
finl=[]
for i in range(dfff.count()):
	d=dict()
	d['Id']=fin[i][0]
	d['rating']=fin[i][1]
	d['age']=fin[i][4]
	finl.append(d)
keyl=[]
finll=[]
for i in range(len(finl)):               # to remove duplicates from join
	if finl[i]['Id'] not in keyl:
		finll.append(finl[i])
		keyl.append(finl[i]['Id'])
df_fin=sqlcontext.createDataFrame(finll)
df_fin.show()

#print(finll)
for col in df_fin.columns:
    if col=="rating":
        df_fin = df_fin.withColumn(col,df_fin[col].cast('float'))
assembler=VectorAssembler(inputCols=['age'],outputCol='features') 
output=assembler.transform(df_fin) 
final_data=output.select('features','rating') 
train_data,test_data=final_data.randomSplit([0.7,0.3]) 
rating_lr=LinearRegression(featuresCol='features',labelCol='rating')
train_rating_model=rating_lr.fit(train_data) 
rating_results=train_rating_model.evaluate(train_data)
unlabeled_data=test_data.select('features')
predictions=train_rating_model.transform(unlabeled_data) 
#pp=sqlcontext.read.json('inp1.json')
#pp.show()
#data=pp.map(lambda x: x.split(','))
#r=data.collect()
#print(r)
#import json

with open('inp1.json') as f:
  data = json.load(f)

match_date=data['date']
team1=data['team1']
team2=data['team2']
#print(date)
team1.pop('name')
team2.pop('name')
t1v=list(team1.values())
t2v=list(team2.values())
t1v=[(x,)for x in t1v]
t2v=[(x,)for x in t2v]	
teams1=sqlcontext.createDataFrame(t1v,["name"])
#teams1.show()
teams2=sqlcontext.createDataFrame(t2v,["name"])
#teams2.show()
teams1final=teams1.join(df2,on=["name"],how='left')
teams2final=teams2.join(df2,on=["name"],how='left')

#teams1final.show()
#teams2final.show()
r1 = teams1final.collect()
l1 =[]
#print(dff.count())
for i in range(teams1final.count()):
	date1 = datetime.datetime.strptime(r1[i][2], "%Y-%m-%d").strftime("%Y,%m,%d")
	date2 = datetime.datetime.strptime(match_date, "%Y-%m-%d").strftime("%Y,%m,%d")
	d1=date1.split(',')
	d2=date2.split(',')
	age=int(d2[0])-int(d1[0])
	#print(age)
	l1.append((r1[i][0],age))
	
#print(l)
ag1=sqlcontext.createDataFrame(l1,["name","age"])
#ag1.show()
teams1final=teams1final.join(ag1,on=['name']).select("Id","age")

r2 = teams2final.collect()
print(r2)
l2 =[]
#print(dff.count())
for i in range(teams2final.count()):
	date1 = datetime.datetime.strptime(r2[i][2], "%Y-%m-%d").strftime("%Y,%m,%d")
	date2 = datetime.datetime.strptime(match_date, "%Y-%m-%d").strftime("%Y,%m,%d")
	d1=date1.split(',')
	d2=date2.split(',')
	age=int(d2[0])-int(d1[0])
	#print(age)
	l2.append((r2[i][0],age,age**2))
	
#print(l)
ag2=sqlcontext.createDataFrame(l2,["name","age"])

#ag2.show()

teams2final=teams2final.join(ag2,on=['name']).select("Id","age")
#teams2final.show()

assembler=VectorAssembler(inputCols=['age'],outputCol='features') 
#assembler.show()
output1=assembler.transform(teams1final) 
output2=assembler.transform(teams2final)
predictions1=train_rating_model.transform(output1)
predictions2=train_rating_model.transform(output2)
predictions1.show()
predictions2.show()
