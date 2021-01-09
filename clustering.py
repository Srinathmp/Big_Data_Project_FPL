from __future__ import print_function
import findspark
findspark.init()
import time
from pyspark import SparkConf,SparkContext
from pyspark.sql import  Row,SQLContext,SparkSession
from pyspark.sql.functions import col
import sys
import requests
from numpy import array
from math import sqrt
#from pyspark.mllib.clustering import KMeans, KMeansModel



import numpy as np
import pandas as pd
#import matplotlib.pyplot as plt
#from mpl_toolkits.mplot3d import Axes3D
#from sklearn.datasets.samples_generator import make_blobs
from pyspark import SparkContext
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SQLContext


# Load and parse the data
conf=SparkConf()
conf.setAppName("FPL_clustering")
sc=SparkContext(master="local[4]",conf=conf)
sqlContext = SQLContext(sc)

FEATURES_COL=['pass_acc','fouls','goals','owngoals','sot']
#data = sc.textFile("player_profile_merge.csv")
#parsedData = data.map(lambda line: list([x.strip('\'()')] for x in line.split(';')))
#print(player_Id.collect())
#c=data.collect()
#print(c)
path = "player_profile_merge.csv"
df = sqlContext.read.csv(path, header=False)
#df.show()
lines = sc.textFile(path)
data = lines.map(lambda line: line.split(","))
df = data.toDF(['id','pass_acc','fouls','goals','owngoals','sot'])

#df.show()

#df_feat = df.select(*(df[c].cast("float").alias(c) for c in df.columns[1:]))
#df_feat.show()
#df_feat.show()
for col in df.columns:
    if col in FEATURES_COL:
        df = df.withColumn(col,df[col].cast('float'))
#df.show()
#df.show()
df = df.na.drop()
vecAssembler = VectorAssembler(inputCols=FEATURES_COL, outputCol="features")
df_kmeans = vecAssembler.transform(df)
df_kmeans=df_kmeans.select('id','features')#.select(df_feat, 'features')
#df_kmeans.select('features').show()
k = 5
kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("features")
model = kmeans.fit(df_kmeans)
centers = model.clusterCenters()

print("Cluster Centers: ")
for center in centers:
    print(center)

transformed = model.transform(df_kmeans).select('id', 'prediction')
rows = transformed.collect()
print('rows = ',rows[:3])
df_pred = sqlContext.createDataFrame(rows)
df_pred.show()
