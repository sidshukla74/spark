


import pyspark

import pandas as pd
import os
#os.chdir(r'C:\Users\sid\Downloads\Code Files')


from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('practice').getOrCreate()

print(spark)

df_pyspark = spark.read.csv('traineddata.csv')




print(df_pyspark)

print(df_pyspark.show())

df_pyspark = spark.read.option('header' ,'true').csv('traineddata.csv')

type(df_pyspark)


df_pyspark.head(3)


df_pyspark.printSchema()


from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('DataFrame').getOrCreate()


spark.read.option('header' , 'true').csv('Iris.csv').show()

print(spark.read.option('header' , 'true').csv('Iris.csv').show())


from pyspark.sql import SparkSession

df_pyspark.printSchema()

df_pyspark = spark.read.option('header', 'true').csv('Iris.csv', inferSchema=True)

df_pyspark

df_pyspark = spark.read.csv('Iris.csv', header=True, inferSchema=True)
df_pyspark.printSchema()


type(df_pyspark)


df_pyspark.columns

df_pyspark.show()

df_pyspark.head(4)



df_pyspark.select('Id').show()


type(df_pyspark.select('Id'))


df_pyspark.select(['Id','Species']).show()



#just show the column data or 
df_pyspark['Species']


df_pyspark.describe()


df_pyspark.describe().show()

#adding columns in dataframe

df_pyspark = df_pyspark.withColumn('Id after adding', df_pyspark['Id']+2).show()








from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
