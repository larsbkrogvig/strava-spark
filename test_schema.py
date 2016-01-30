from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

# Configure application
conf = (SparkConf()
         .setMaster("local")
         .setAppName("Strava analyze"))

# Initialize contexts
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

# Read data
df = sqlContext.read.format('com.databricks.spark.xml') \
        .options(rowTag='trkpt') \
        .load('activity.xml')

# Show schema
df.printSchema()

