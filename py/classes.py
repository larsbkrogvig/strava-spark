import os
import re
import glob
import pickle
import boto3

from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext, DataFrame
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
                 hiveContext=None,
                 conf=(SparkConf().setAppName('Strava analysis')),
                 filter_bug_inducing_rows=True
                 ):

        ''' Initialize Strava Analysis object'''


        # INPUT PARAMETERS

        self.athletes = athletes # Athletes to analyze (optional)
        self.activity_types = activity_types # Activity_types to consider (default)
        self.filter_bug_inducing_rows = filter_bug_inducing_rows


        # CONFIGURE SPARK

        if sc != None and hiveContext != None: # Both contexts were supplied by user
            print 'Info: Using supplied SparkContext and HiveContext'
            self.sc = sc
            self.hiveContext = hiveContext

        else: # Initialize new contexts
            print 'Info: Intitializing SparkContext and hiveContext from (default) conf'
            self.sc = SparkContext(conf=conf)
            self.hiveContext = HiveContext(self.sc)

        self.schema = pickle.load(open('./schema.p', 'rb')) # The pre-defined schema
        self.df = None # Empry DataFrame to be populated later


        # CONFIGURE DATA SOURCE

        data_root_path = {
                's3': 's3n://%s/%s/' % (s3bucket, activity_directory), 
                'local': './%s/' % activity_directory
        }
        
        if data_source not in data_root_path.keys(): # Check if data source is valid 
            raise Exception(('Unrecognized data source %s. '
                             'Supported sources: "%s".') \
                             % '", "'.join(data_root_path.keys()))
        
        self.data_source = data_source # This is a valid data source
        self.path = data_root_path[data_source] # This is the path to the data


        # (S3 SPECIFIC STUFF)

        if data_source == 's3':

            # Get a list of files in he activity_directorys
            bucket = boto3.resource('s3').Bucket(s3bucket) 
            objects = bucket.objects.filter(Prefix='%s/gpx/' % activity_directory)
            files = [obj.key for obj in objects] 

            # Make set of observed combinations of athlete and activity_type
            athlete_and_type = set([]) # Empty set to populate
            fpattern = '\/([\w]+)\/(?:[\w-]+)-([\w]+)\.gpx' # File name pattern
            for fname in files:
                match = re.match(activity_directory+'/gpx'+fpattern, fname)
                if match:
                    athlete_and_type.add((match.group(1), match.group(2)))

            self.s3_athlete_and_type = athlete_and_type # Save set for later use

        pass


    def _get_athlete_directories(self):
        '''
        Look for athlete directories in data_root_path \
        and update self.athletes
        '''

        if self.data_source in ['local']:

            self.athletes = [
                directory for directory in os.listdir(self.path+'gpx/')
                if re.match('^[\w-]+$', directory)
            ]

        else:
            print ('Warning: Automatic directory/athlete detection not yet supported for '
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
            return glob.glob(self.path+'gpx/%s/*%s.gpx' % (athlete, activity_type))

        # Check if combination exists by using previously compiled sets
        elif self.data_source == 's3':
            return ((athlete, activity_type) in self.s3_athlete_and_type)

    def _load_dataset(self):
        '''
        Loads strava activities from source to DataFrame self.df
        '''

        # Get athlete list if not already set
        if not self.athletes:
            self._get_athlete_directories()

        # Initialize empty dataset
        self.df = self.hiveContext.createDataFrame(
            self.sc.emptyRDD(),
            self.schema
        )

        for athlete in self.athletes:
            for activity_type in self.activity_types:
        
                # Check that there are files of that type (or else .load fails)
                if self._activities_exist(athlete, activity_type):

                    # Read data
                    dfadd = self.hiveContext.read.format('com.databricks.spark.xml') \
                                    .options(rowTag='trkpt', treatEmptyValuesAsNulls=False) \
                                    .schema(self.schema) \
                                    .load(self.path+'gpx/%s/*%s.gpx' % (athlete, activity_type))
                
                    dfadd = dfadd.withColumn('athlete', lit(athlete)) \
                                 .withColumn('activity_type', lit(activity_type))
                
                    self.df = self.df.unionAll(dfadd)

        if self.filter_bug_inducing_rows:
            self.df = self.df.filter(self.df['extensions.gpxtpx:TrackPointExtension.#VALUE'].isNull())

        pass


    def derive_schema(self):
        '''
        Loads all data in self.path and derives the schema, saves with pickle to "schema.p"
        '''

        df = self.hiveContext.read.format('com.databricks.spark.xml') \
                    .options(rowTag='trkpt') \
                    .load(self.path+'gpx/*')

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

