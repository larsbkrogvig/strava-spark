from csv import DictReader
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, DataFrame
from pyspark.sql.functions import lit

from describe_data_local import describe_data_local

import pickle
import os
import fnmatch

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

# Read credentials
#with open('s3credentials.csv') as csvfile:
#    reader = DictReader(csvfile, delimiter=',')
#    row = next(reader, None)
#    s3AccessKeyId = row['Access Key Id']
#    s3SecretAccessKey = row['Secret Access Key']

# Configure s3 credentials
#if mode == 's3':
#    sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", s3AccessKeyId)
#    sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", s3SecretAccessKey)

# Read pre-prepared descriptions of data
desc = describe_data_local()
schema = pickle.load(open('schema.p', 'rb'))

df = sqlContext.createDataFrame(sc.emptyRDD(), schema)

for athlete in desc['athletes']:
    for activity_type in desc['activity_types']:

        directory = '%s/strava-activities-subset/%s/' % (FILE_PATH, athlete)

        print athlete, activity_type
        print [f for f in os.listdir(directory) if fnmatch.fnmatch(f, '*%s.gpx' % activity_type)]
        if len([f for f in os.listdir(directory) if fnmatch.fnmatch(f, '*%s.gpx' % activity_type)]):
    
            # Read data
            dfadd = sqlContext.read.format('com.databricks.spark.xml') \
                        .options(rowTag='trkpt') \
                        .schema(schema) \
                        .load('%s/strava-activities-subset/%s/*%s.gpx' % (FILE_PATH, athlete, activity_type)) # 
        
            dfadd = dfadd.withColumn('athlete', lit(athlete)) \
                         .withColumn('activity_type', lit(activity_type))
        
            dfadd.show()
        
            df = df.unionAll(dfadd)

# Show schema
df.show(50)
df.printSchema()
