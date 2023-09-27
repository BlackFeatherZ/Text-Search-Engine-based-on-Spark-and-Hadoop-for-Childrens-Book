from pyspark import SparkConf, SparkContext 
from pyspark.sql import SparkSession, SQLContext 
import os, re, sys, math

#sc = SparkContext.getOrCreate(SparkConf()) 
#sql = SQLContext(sc)

#You can also submit the job to a spark cluster, e.g.: 
spark = SparkSession.builder.master('spark://dpw2frankm:7077').appName('searchcall').getOrCreate() 
sc = spark.sparkContext 
sql = SQLContext(sc) 

#Be careful, in case of spark cluster, the tf-idf index should be in an HDFS, e.g.: 
tfidf_RDD = sql.read.parquet('hdfs://localhost:9000/user/root/tfidf-index') .rdd.map(lambda x: (x['_2'],(x['_1'],x['_3']))) 

#tfidf_RDD = sql.read.parquet("tfidf-index").rdd.map(lambda x: (x['_2'],(x['_1'],x['_3']))) 

def tokenize(s): 
  return re.split("\\W+", s.lower())

def search(query, topN): 
  tokens = sc.parallelize(tokenize(query)).map(lambda x: (x, 1) ).collectAsMap() 
  bcTokens = sc.broadcast(tokens) 
 
  joined_tfidf = tfidf_RDD.map(lambda x: (x[0], bcTokens.value.get(x[0], '-'), x[1]) ).filter(lambda x: x[1] != '-' ) 
   
  scount = joined_tfidf.map(lambda a: a[2]).aggregateByKey((0,0), 
  (lambda acc, value: (acc[0] + value, acc[1] + 1)), 
  (lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])) ) 
   
  scores = scount.map(lambda x: ( x[1][0]*x[1][1]/len(tokens), x[0]) ).top(topN) 

  return topN,scores
