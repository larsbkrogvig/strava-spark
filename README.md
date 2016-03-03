> Work in progress! 

> The instructions in this readme are incomplete.

# strava-spark
Analyzing my Strava history with Spark

## Prerequisites
* Apache Spark
* Strava activities stored locally or in an s3 bucket
* Other bits and pieces (_TODO_)

Activities are stored in the following structure
* _strava-activities_
  * _athlete1_
    * <_.gpx-files_>
  * _athlete2_
    * <_.gpx-files_>
  * ...

The _.gpx_ files follow this naming convention:
> [id]-[activity type].gpx

Typical _activity types_ are:
* Ride
* Run
* NordicSki

### Loading the dataset
In a `pyspark` program like `analysis.py`:
```
from classes import StravaLoader
df = StravaLoader().get_dataset()
```
In a `pyspark` shell:
```
from classes import StravaLoader
df = StravaLoader(sc=sc, hiveContext=sqlContext).get_dataset()
```