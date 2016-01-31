from csv import DictReader
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

mode = 's3'
#mode = 'local'

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

# Read data
df = sqlContext.read.format('com.databricks.spark.xml') \
        .options(rowTag='trkpt') \
        .load('%s/strava-activities/lkrogvig/20140824-075956-Ride.gpx' % FILE_PATH) # 

# Show schema
df.printSchema()

