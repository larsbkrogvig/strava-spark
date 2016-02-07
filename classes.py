import os
import re
import glob
import pickle
import subprocess

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, DataFrame
from pyspark.sql.types import StringType
from pyspark.sql.functions import lit

class StravaLoader(object):

    def __init__(self, 
                 data_source='local', 
                 activity_directory='strava-activities-subset',
                 s3bucket='larsbk',
                 athletes=None,
                 activity_types=[
                    'Ride',
                    'Run',
                    'NordicSki'
                 ],
                 sc=None,
                 sqlContext=None,
                 conf=(SparkConf().setAppName('Strava analysis'))
                 ):

        '''
        Initialize Strava Analysis object
        '''

        data_root_path = {
                's3': 's3n://%s/%s/' % (s3bucket, activity_directory), 
                'local': './%s/' % activity_directory
        }

        # Check if valid data source
        if data_source not in data_root_path.keys():
            raise Exception(('Unrecognized data source %s. '
                             'Supported sources: "%s".') \
                             % '", "'.join(data_root_path.keys()))

        # Spark contexts
        if sc != None and sqlContext != None:
            print 'Info: Using supplied SparkContext and SQLContext'
            self.sc = sc
            self.sqlContext = sqlContext
        else:
            print 'Info: Intitializing SparkContext and sqlContext from (default) conf'
            self.sc = SparkContext(conf=conf)
            self.sqlContext = SQLContext(self.sc)

        # Input attributes
        self.data_source = data_source
        self.path = data_root_path[data_source]
        self.athletes = athletes
        self.activity_types = activity_types

        # Dataframe
        self.schema = pickle.load(open('./schema.p', 'rb'))
        self.df = None

        pass

    def _get_athlete_directories(self):
        '''
        Look for athlete directories in data_root_path \
        and update self.athletes
        '''

        if self.data_source in ['local']:

            self.athletes = [
                directory for directory in os.listdir(self.path)
                if re.match('^[\w-]+$', directory)
            ]

        else:
            print ('Warning: Automatic directory/athlete detection not supported for '
                   'data source %s. Using: "akrogvig", "lkrogvig", "brustad"') \
                   % self.data_source

            self.athletes = ['akrogvig', 'lkrogvig', 'brustad']

        pass

    def _activities_exist(self, athlete, activity_type):
        '''
        Checks if there exists activities of type <activity_type> for athlete <athlete>, 
        returns a boolean value
        '''

        # Check local directory with glob
        if self.data_source == 'local':
            return glob.glob(self.path+'%s/*%s.gpx' % (athlete, activity_type))

        # Check s3 bucket with aws cli
        elif self.data_source == 's3':

            # List entire athlete subdirectory
            ls = subprocess.Popen([
                'aws',
                's3', 
                'ls',
                's3://larsbk/strava-activities/%s/' % athlete
                ],
                stdout=subprocess.PIPE).communicate()[0].split('\n')

            # Look for activity with correct type
            for lsline in ls:
                if re.match('.*%s.gpx' % activity_type, lsline):
                    return True
            return False

    def _load_dataset(self):
        '''
        Loads strava activities from source to DataFrame self.df
        '''

        # Get athlete list if not already set
        if not self.athletes:
            self._get_athlete_directories()

        # Initialize empty dataset
        self.df = self.sqlContext.createDataFrame(
            self.sc.emptyRDD(),
            self.schema
        )

        for athlete in self.athletes:
            for activity_type in self.activity_types:
        
                # Check that there are files of that type (or else .load fails)
                if self._activities_exist(athlete, activity_type):

                    # Read data
                    #treatEmptyValuesAsNulls=True
                    dfadd = self.sqlContext.read.format('com.databricks.spark.xml') \
                                    .options(rowTag='trkpt', failFast=False) \
                                    .schema(self.schema) \
                                    .load(self.path+'%s/*%s.gpx' % (athlete, activity_type))
                
                    dfadd = dfadd.withColumn('athlete', lit(athlete)) \
                                 .withColumn('activity_type', lit(activity_type))
                
                    self.df = self.df.unionAll(dfadd)

        pass

    def derive_schema(self):
        '''
        Loads all data in self.path and derives the schema, saves with pickle to "schema.p"
        '''

        df = self.sqlContext.read.format('com.databricks.spark.xml') \
                    .options(rowTag='trkpt') \
                    .load(self.path)

        df = df.withColumn('athlete',lit(None).cast(StringType())) \
               .withColumn('activity_type',lit(None).cast(StringType()))

        df.printSchema()
        pickle.dump(df.schema, open("schema.p", "wb"))

        pass

    def get_dataset(self):
        '''
        Returns strava activity dataset
        '''
        if not self.df:
            self._load_dataset()
        
        return self.df

