# Configure the necessary Spark environment
import os
import sys

spark_home = '/usr/lib/spark/'
os.environ["SPARK_HOME"] = spark_home # Is this necessary?

pyspark_submit_args = '--jars /home/hadoop/spark-xml_2.10-0.3.2.jar'

if not "pyspark-shell" in pyspark_submit_args: pyspark_submit_args += " pyspark-shell"
os.environ["PYSPARK_SUBMIT_ARGS"] = pyspark_submit_args

sys.path.insert(0, spark_home + "/python")

# Add the py4j to the path.
# You may need to change the version number to match your install
sys.path.insert(0, os.path.join(spark_home, 'python/lib/py4j-0.9-src.zip'))

# Initialize PySpark to predefine the SparkContext variable 'sc'
execfile(os.path.join(spark_home, 'python/pyspark/shell.py'))

