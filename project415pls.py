import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from sklearn.cluster import DBSCAN
from sklearn.linear_model import LinearRegression
import numpy as np
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark import sql
from pyspark.mllib.clustering import PowerIterationClustering
from pyspark.mllib.clustering import PowerIterationClusteringModel
from pyspark.sql import Row
from pyspark.sql.types import FloatType
from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.sql import Row
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler



columns= ['streamerID', 'currentViews', 'streamCreatedTime', 'GameName',
           'broadCasterID', 'broadCasterName', 'delaySetting', 'followerNumber',
           'partnerStatus', 'broadcasterLanguage', 'totalViews', 'language',
           'broadcastersCreatedTime', 'playBackBitRate', 'SourceResolution', 'NaN']

df = pd.read_csv('datatest.csv', sep = ',',names = columns, encoding='unicode_escape')

df.dropna(subset=['broadCasterName', 'GameName'], inplace=True)
df[['streamerID', 'currentViews', 'totalViews', 'followerNumber', 'broadCasterID']] = df[['streamerID', 'currentViews', 'totalViews', 'followerNumber', 'broadCasterID']].fillna(0) # Fill odddly missing values
df.dropna(axis=1, inplace=True) # Remove all incomplete columns
df.dropna(inplace=True)
df


df.dtypes



df["totalViews"] = df['totalViews'].astype('float')
df["broadCasterID"] = df['broadCasterID'].astype('float')
df.dtypes


clustering = DBSCAN(eps=3, min_samples=5).fit(df[['streamerID', 'broadCasterID', 'totalViews']]) dataframe = (df-df.mean())/df.std()
dataframe['cluster'] = clustering.labels_
df['cluster'] = clustering.labels_
df[df['cluster'] != -1].to_csv('cluster.csv')



dataframe.drop(['broadCasterName', 'GameName', 'streamCreatedTime', 'partnerStatus', 'broadcasterLanguage', 'language', 'broadcastersCreatedTime', 'SourceResolution'], axis=1, inplace=True)
print(dataframe.shape)
print(dataframe)


spark = SparkSession
spark = SparkSession.builder
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)
data = df2
spdf = sqlContext.createDataFrame(data)
spdf.cache()
spdf.printSchema()
spdf.describe().toPandas().transpose()
FEATURES_COL =['streamCreatedTime', 'GameName',
           'broadCasterID', 'broadCasterName', 'delaySetting',
           'partnerStatus', 'broadcasterLanguage', 'language',
           'broadcastersCreatedTime', 'playBackBitRate', 'SourceResolution']
OUT = ['streamerID', 'currentViews', 'totalViews', 'followerNumber']
vectorassembler = VectorAssembler(inputCols= FEATURES_COL,outputCol= 'features')
vector = vectorassembler.transform(spdf).select(['features','streamerID','currentViews', 'totalViews', 'followerNumber'])

vector.show()

linear = LinearRegression(featuresCol= 'features', labelCol='streamerId')
linear2  = LinearRegression(featuresCol= 'features', labelCol='currentViews')
linear3  = LinearRegression(featuresCol= 'features', labelCol='totalViews')
linear4  = LinearRegression(featuresCol= 'features', labelCol='followerNumber')
print(linear)
