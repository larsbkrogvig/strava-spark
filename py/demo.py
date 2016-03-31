# Pyspark commands
from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext

sc = SparkContext(conf=SparkConf().setAppName("Demo"))
sqlContext = HiveContext(sc)

df = sqlContext.read\
               .format('com.databricks.spark.xml')\
               .options(rowTag='trkpt')\
               .load('s3://larsbk/strava-activities-subset/gpx/*')

df.printSchema()

df.count()

df.show()

df.select([
    'time',
    'extensions.gpxtpx:TrackPointExtension.gpxtpx:hr'
    ]).show()

df.groupBy('ele').count().orderBy('count', ascending=False).show()