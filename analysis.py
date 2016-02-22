from pyspark.sql import Window
from pyspark.sql.functions import lag, unix_timestamp
from classes import StravaLoader

def main():

    df = StravaLoader('local').get_dataset()
    df.printSchema()

    # Convert timestamp to seconds
    df = df.withColumn('unix_time', unix_timestamp(df['time'], "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    
    # Window for time difference
    window = Window.partitionBy('athlete', 'activity_type').orderBy('unix_time')
    df = df.withColumn('unix_time_prev', lag('unix_time', count=1).over(window))
    df = df.withColumn('unix_time_diff', df['unix_time'] - df['unix_time_prev'])
    
    # Select prev time and alias fields
    df = df.select( 
        df['@lat'].alias('lat'), 
        df['@lon'].alias('long'), 
        df['ele'].alias('ele'), 
        df['extensions.gpxtpx:TrackPointExtension.gpxtpx:atemp'].alias('atemp'), 
        df['extensions.gpxtpx:TrackPointExtension.gpxtpx:cad'].alias('cad'), 
        df['extensions.gpxtpx:TrackPointExtension.gpxtpx:hr'].alias('hr'), 
        df['time'].alias('time'), 
        df['unix_time'].alias('unix_time'), 
        df['unix_time_diff'].alias('unix_time_diff'), 
        df['athlete'].alias('athlete'), 
        df['activity_type'].alias('activity_type') 
    )
    
    df.orderBy('unix_time_diff', ascending=False).withColumn('hrs', df['unix_time_diff']/(60*60)).show()

    pass

if __name__ == '__main__':
    main()


