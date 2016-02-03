from csv import DictReader
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import lit

import pickle

#mode = 's3'
mode = 'local'

# Set root file path
filepath = {'s3': 's3n://larsbk/', 'local': '.'}
FILE_PATH = filepath[mode]

# Configure application
conf = (SparkConf().setAppName("Strava analyze"))
         #.setMaster("local")
         
# Initialize contexts
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

df = sqlContext.read.format('com.databricks.spark.xml') \
        .options(rowTag='trkpt') \
        .load('%s/strava-activities/*' % FILE_PATH) #

df = df.withColumn('athlete', lit('<athlete>')) \
       .withColumn('activity_type', lit('<activity_type>'))

pickle.dump(df.schema, open("schema.p", "wb"))
