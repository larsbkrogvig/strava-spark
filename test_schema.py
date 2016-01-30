from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

df = sqlContext.read.format('com.databricks.spark.xml') \
        .options(rowTag='trkpt') \
        .load('activity.xml')

df.printSchema()